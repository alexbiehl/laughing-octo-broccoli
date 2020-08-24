use super::metrics::Metrics;
use super::protocol;

use async_std::net::ToSocketAddrs;
use async_std::sync::RwLock;
use async_std::task;
use slog;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Status;

/// Communicates with the GRPCBackend and runs tasks.
pub struct Executor<ForkScheduler, TaskRunner> {
    state: Arc<State<ForkScheduler, TaskRunner>>,
}

/// We need to manually specify Clone as Rusts auto-derive cannot
/// figure out that TaskRunner wouldn't need a Clone constraint.
impl<ForkScheduler, TaskRunner> Clone for Executor<ForkScheduler, TaskRunner> {
    fn clone(&self) -> Self {
        Executor {
            state: self.state.clone(),
        }
    }
}

/// Executor internal representation of a Ticket.
/// TODO needs priority
struct Ticket {
    batch_id: u64,
    scheduler: SocketAddr,
}

struct State<ForkScheduler, TaskRunner> {
    logger: slog::Logger,
    /// Socket address to reach this executor.
    address: SocketAddr,
    /// Decodes and runs tasks.
    task_runner: TaskRunner,
    /// A bunch of open connections to schedulers. Connections can only be used
    /// by one thread at a time.
    /// Clients are cheap to clone and don't require any additional synchronization.
    known_schedulers: RwLock<
        HashMap<SocketAddr, protocol::scheduler_backend_client::SchedulerBackendClient<Channel>>,
    >,
    /// Queue to hold the tickets
    /// TODO: Have this be a proper priority queue.
    queue: RwLock<VecDeque<Ticket>>,
    /// Mechasnism to create fork new schedulers.
    fork_scheduler: ForkScheduler,
    /// Metrics object
    metrics: Metrics,
}

impl<ForkScheduler, TaskRunner> Executor<ForkScheduler, TaskRunner>
where
    TaskRunner: 'static + crate::task_runner::TaskRunner,
    ForkScheduler: 'static + crate::task_runner::ForkScheduler,
{
    pub fn new(
        logger: slog::Logger,
        fork_scheduler: ForkScheduler,
        task_runner: TaskRunner,
        address: SocketAddr,
    ) -> Self {
        let state = Arc::new(State {
            known_schedulers: RwLock::new(HashMap::new()),
            queue: RwLock::new(VecDeque::new()),
            logger: logger.new(slog::o!("executor" => address)),
            metrics: Metrics::new(),
            address,
            task_runner,
            fork_scheduler,
        });

        let executor = Self { state };

        // Spawn a bunch of workers that drain the queue
        // and communicate with schedulers.
        // TODO Configurable number of workers
        (1..5).for_each(|i| {
            let executor1 = executor.clone();
            tokio::spawn(async move {
                executor1.worker_loop(i).await;
            });
        });

        executor
    }

    async fn worker_loop(self, worker_id: u32) {
        loop {
            // TODO we want this loop to have an end
            // eventually
            // TODO Handle connection errors gracefully.
            let result = self.worker(worker_id).await;
            match result {
                Ok(()) => {}
                Err(error) => {
                    slog::error!(self.state.logger, "Error happened";
                                 "worker" => worker_id, "error" => error.to_string()
                    );
                }
            }
        }
    }

    /// Worker loop that takes tickets from the queue and
    /// requests them from upstream.
    /// TODO:
    ///   - Make sure to post status updates to scheduler
    async fn worker(&self, worker_id: u32) -> Result<(), Box<dyn Error>> {
        let (todo_ticket, queue_size) = async {
            let mut queue = self.state.queue.write().await;
            let ticket = queue.pop_front();
            let queue_size = queue.len();
            (ticket, queue_size)
        }
        .await;

        if let None = todo_ticket {
            // TODO We are in deparate need of something like semaphore
            task::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        let ticket = todo_ticket.ok_or(Status::internal("ticket can't be None"))?;

        slog::debug!(self.state.logger, "Redeeming ticket";
                     "worker" => worker_id,
                     "batch_id" => ticket.batch_id,
                     "scheduler" => ticket.scheduler,
                     "queue_size" => queue_size
        );

        let mut scheduler_client = async {
            let known_schedulers = self.state.known_schedulers.read().await;
            known_schedulers
                .get(&ticket.scheduler)
                .map(|client| client.clone())
        }
        .await
        .ok_or(Status::internal("Ticket without scheduler"))?;

        let redeem_ticket_response = scheduler_client
            .redeem_ticket(protocol::RedeemTicketRequest {
                executor_address: Some(self.state.address.into()),
                batch_id: Some(protocol::BatchId {
                    scheduler: Some(ticket.scheduler.into()),
                    id: ticket.batch_id,
                }),
            })
            .await?;

        self.state.metrics.inc_total_redeems();

        let mut redeem_response = redeem_ticket_response
            .into_inner()
            .redeem_response
            .ok_or(Status::invalid_argument("No redeem_response present"))?;

        loop {
            match redeem_response {
                protocol::redeem_ticket_response::RedeemResponse::SpawnTask(spawn_task) => {
                    let task_id = spawn_task
                        .task_id
                        .ok_or(Status::invalid_argument("task_id not present"))?;

                    let local_task_id = task_id.local_id;

                    let decoded_task = self.state.task_runner.decode_task(spawn_task.task);

                    let task = if let Some(task) = decoded_task {
                        task
                    } else {
                        slog::error!(self.state.logger, "Error decoding task";
                                     "worker" => worker_id,
                                     "task_id" => local_task_id,
                                     "scheduler" => ticket.scheduler,
                        );

                        self.state.metrics.inc_total_failed_tasks();

                        // Argl, let's tell the scheduler that we failed to decode
                        // the task.
                        scheduler_client
                            .complete_task(protocol::CompleteTaskRequest {
                                executor_address: Some(self.state.address.into()),
                                task_id: Some(task_id),
                                result: Some(
                                    protocol::complete_task_request::Result::DecodeTaskError(
                                        protocol::DecodeTaskError {},
                                    ),
                                ),
                                next_redeem: None,
                            })
                            .await?;

                        // All cool so far, let's continue with the next one.
                        break Ok(());
                    };

                    let encoded_result = match self
                        .state
                        .task_runner
                        .run_task(&self.state.fork_scheduler, task)
                        .await
                    {
                        Ok(result) => {
                            self.state.metrics.inc_total_successful_tasks();
                            protocol::complete_task_request::Result::EncodedResult(
                                self.state.task_runner.encode_result(result),
                            )
                        }
                        Err(error) => {
                            self.state.metrics.inc_total_failed_tasks();
                            protocol::complete_task_request::Result::ErrorMessage(error.to_string())
                        }
                    };

                    // Check if the next ticket is also detined for the same scheduler.
                    // Redeem it in one go.
                    let (next_ticket, is_empty) = async {
                        let mut queue = self.state.queue.write().await;
                        let queue_size = queue.len();
                        if let Some(next_ticket) = queue.get(0) {
                            if ticket.scheduler == next_ticket.scheduler {
                                slog::debug!(self.state.logger, "Redeeming ticket";
                                             "worker" => worker_id,
                                             "batch_id" => next_ticket.batch_id,
                                             "scheduler" => next_ticket.scheduler,
                                             "queue_size" => queue_size - 1
                                );

                                // We count "redeems-in-one-go" as redeem too!
                                self.state.metrics.inc_total_redeems();
                                (queue.pop_front(), false)
                            } else {
                                (None, false)
                            }
                        } else {
                            self.state.metrics.inc_total_found_empty_queue();
                            (None, true)
                        }
                    }
                    .await;

                    let response = scheduler_client
                        .complete_task(protocol::CompleteTaskRequest {
                            executor_address: Some(self.state.address.into()),
                            task_id: Some(task_id),
                            result: Some(encoded_result),
                            next_redeem: next_ticket.as_ref().map(|next_ticket| {
                                protocol::BatchId {
                                    scheduler: Some(next_ticket.scheduler.into()),
                                    id: next_ticket.batch_id,
                                }
                            }),
                        })
                        .await?;

                    slog::debug!(self.state.logger, "Running task complete";
                                 "worker" => worker_id,
                                 "task_id" => local_task_id
                    );

                    if is_empty {
                        slog::warn!(self.state.logger, "Found empty queue, waiting for tickets";
                                     "worker" => worker_id
                        );
                    }

                    if let Some(spawn_task) = response.into_inner().spawn_task {
                        redeem_response =
                            protocol::redeem_ticket_response::RedeemResponse::SpawnTask(spawn_task);
                    } else {
                        // Only count this as expired in case we tried
                        // redeeming-in-one-go
                        if next_ticket.is_some() {
                            self.state.metrics.inc_total_expired_tickets();
                            slog::debug!(self.state.logger, "Ticket expired";
                                         "worker" => worker_id
                            );
                        }
                        break Ok(());
                    }
                }
                protocol::redeem_ticket_response::RedeemResponse::TicketExpired(
                    _ticket_expired,
                ) => {
                    self.state.metrics.inc_total_expired_tickets();
                    slog::debug!(self.state.logger, "Ticket expired";
                                 "worker" => worker_id
                    );
                    break Ok(());
                }
            }
        }
    }

    /// Run the executor.
    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        let executor_service_server =
            protocol::executor_service_server::ExecutorServiceServer::new(self.clone());

        // TODO Ugh
        let bind = ("::0", self.state.address.port())
            .to_socket_addrs()
            .await
            .expect("Could not resolve socket")
            .next()
            .expect("No socket");

        let serve = tonic::transport::Server::builder()
            .add_service(executor_service_server)
            .serve(bind);

        slog::info!(self.state.logger, "Serving"; "address" => self.state.address);

        serve.await?;
        Ok(())
    }

    /// Register with a new Scheduler
    pub async fn add_scheduler(
        &self,
        socket_addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Don't hold the lock on known_schedulers while connecting to it
        // There might be the chance of creating the scheduler connection
        // multiple times but it's generally fine.

        let is_new_scheduler = async {
            let known_schedulers = self.state.known_schedulers.read().await;
            !known_schedulers.get(&socket_addr).is_some()
        }
        .await;

        if is_new_scheduler {
            slog::debug!(self.state.logger, "Adding new scheduler";
                         "scheduler" => socket_addr
            );

            let endpoint = Endpoint::from_shared(format!("http://{}", socket_addr))?;

            slog::debug!(self.state.logger, "Connecting grpc client...");

            let mut client =
                protocol::scheduler_backend_client::SchedulerBackendClient::connect(endpoint)
                    .await
                    .expect("Failed to connect grpc client");

            let mut known_schedulers = self.state.known_schedulers.write().await;

            if let None = known_schedulers.get(&socket_addr) {
                slog::debug!(self.state.logger, "Sending RegisterRequest";
                             "scheduler" => socket_addr
                );

                client
                    .register(protocol::RegisterRequest {
                        executor_address: Some(self.state.address.into()),
                    })
                    .await?;

                known_schedulers.insert(socket_addr, client);
            }
        }
        Ok(())
    }

    /// Scheduler sent a ticket to us, put it ont our
    /// queue.
    async fn enqueue_ticket(self, ticket: Ticket) {
        tokio::spawn(async move {
            self.add_scheduler(ticket.scheduler)
                .await
                .map_err(|error| {
                    slog::error!(self.state.logger, "Failed to add scheduler";
                                 "error" => error.to_string()
                    );
                })
                .expect("Failed to add scheduler");

            let batch_id = ticket.batch_id;

            slog::debug!(self.state.logger, "Enqueing ticket";
                         "batch_id" => batch_id
            );

            // Local scoping for short critical sections
            let mut queue = self.state.queue.write().await;
            queue.push_back(ticket);

            self.state.metrics.inc_total_queued_tickets();
            slog::debug!(self.state.logger, "Enqueing ticket complete";
                         "batch_id" => batch_id
            );
        });
    }
}

#[tonic::async_trait]
impl<ForkScheduler, TaskRunner> protocol::executor_service_server::ExecutorService
    for Executor<ForkScheduler, TaskRunner>
where
    TaskRunner: 'static + crate::task_runner::TaskRunner,
    ForkScheduler: 'static + crate::task_runner::ForkScheduler,
{
    type StatusUpdateStream =
        tokio::sync::mpsc::Receiver<Result<protocol::StatusUpdateResponse, tonic::Status>>;

    async fn new_ticket(
        &self,
        request: tonic::Request<protocol::NewTicketRequest>,
    ) -> Result<tonic::Response<protocol::Empty>, tonic::Status> {
        let new_ticket_request = request
            .into_inner()
            .batch_id
            .ok_or(Status::invalid_argument("batch_id not present"))?;

        let batch_id = new_ticket_request.id;

        let scheduler = SocketAddr::try_from(
            new_ticket_request
                .scheduler
                .ok_or(Status::invalid_argument("scheduler address not present"))?,
        )?;

        self.clone()
            .enqueue_ticket(Ticket {
                batch_id,
                scheduler,
            })
            .await;

        Ok(tonic::Response::new(protocol::Empty {}))
    }

    async fn status_update(
        &self,
        request: tonic::Request<protocol::StatusUpdateRequest>,
    ) -> Result<tonic::Response<Self::StatusUpdateStream>, tonic::Status> {
        let status_update_request = request.into_inner();

        let _scheduler = SocketAddr::try_from(
            status_update_request
                .scheduler
                .ok_or(Status::invalid_argument("scheduler address not present"))?,
        )?;

        let interval = Duration::from_secs(status_update_request.interval_seconds);

        // TODO bound of 4 is pretty arbitrary
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        let self1 = self.clone();

        tokio::spawn(async move {
            loop {
                slog::debug!(self1.state.logger, "Emitting status update");
                if let Ok(()) = tx
                    .send(Ok(self1.state.metrics.to_status_update_response()))
                    .await
                {
                    task::sleep(interval).await;
                } else {
                    slog::info!(self1.state.logger, "");
                    // We weren't succesful in sending the value. That
                    // means the stream got closed.
                    break;
                }
            }
        });
        Ok(tonic::Response::new(rx))
    }
}
