pub mod executor;
pub mod metrics;
pub mod protocol;

pub use executor::Executor;
pub use metrics::SampledMetrics;

use crate::scheduler;
use crate::scheduler::FailedReason;
use crate::scheduler::Offer;
use crate::scheduler::RedeemTicketError;
use crate::scheduler::Scheduler;
use crate::scheduler::SchedulerConfig;
use crate::scheduler::TaskCoordinator;
use crate::scheduler::TaskId;

use async_std::sync::Mutex;
use async_std::sync::RwLock;
use async_std::task;
use slog;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tonic::transport::channel::Channel;
use tonic::transport::Endpoint;
use tonic::Status;

/// Configuration for the GRPC backend.
pub struct Config {
    /// Public facing address to be used to reach the scheduler.
    pub service_addr: SocketAddr,
    /// Address to bind the listener socket to.
    pub bind_addr: SocketAddr,
    /// Internval in which to send the updates.
    pub heartbeat_interval: Duration,
}

/// Scheduler Backend using GRPC as a transport.
#[repr(transparent)]
pub struct GrpcBackend<TaskPlacer, Coordinator>
where
    TaskPlacer: 'static + crate::scheduler::TaskPlacer,
    Coordinator: 'static + TaskCoordinator,
{
    state: Arc<State<TaskPlacer, Coordinator>>,
}

/// #[derive(Clone)] doesn't cut it with the type parameters
/// thus we need a manual instance.
impl<TaskPlacer, Coordinator> Clone for GrpcBackend<TaskPlacer, Coordinator>
where
    TaskPlacer: 'static + crate::scheduler::TaskPlacer,
    Coordinator: 'static + TaskCoordinator,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

struct ExecutorState {
    // An always open connection to the executor. A client is very cheap to clone.
    // No need for additional synchronization.
    client: protocol::executor_service_client::ExecutorServiceClient<Channel>,
}

/// Internal state of the Backend.
struct State<TaskPlacer, Coordinator>
where
    TaskPlacer: 'static + crate::scheduler::TaskPlacer,
    Coordinator: 'static + TaskCoordinator,
{
    config: Config,
    logger: slog::Logger,
    scheduler: Scheduler<GrpcBackend<TaskPlacer, Coordinator>, TaskPlacer, Coordinator>,
    /// Known executors and open connections to them.
    known_executors: RwLock<HashMap<SocketAddr, ExecutorState>>,
    /// Aggregated executor metrics
    executor_metrics: RwLock<SampledMetrics>,
    /// We want to limit the amount of concurrent ticketing
    /// to avoid overwhelming the system
    concurrent_ticketings: Mutex<usize>,
}

impl<TaskPlacer, Coordinator> scheduler::Executor for GrpcBackend<TaskPlacer, Coordinator>
where
    TaskPlacer: crate::scheduler::TaskPlacer,
    Coordinator: TaskCoordinator,
{
    type Id = SocketAddr;
}

impl<TaskPlacer, Coordinator> GrpcBackend<TaskPlacer, Coordinator>
where
    TaskPlacer: crate::scheduler::TaskPlacer,
    Coordinator: TaskCoordinator,
{
    pub fn new(
        config: Config,
        scheduler_config: SchedulerConfig,
        task_placer: TaskPlacer,
        coordinator: Coordinator,
    ) -> Self
    where
        TaskPlacer: Clone,
    {
        let logger = scheduler_config
            .get_logger()
            .new(slog::o!("scheduler" => config.service_addr));

        let adjusted_scheduler_config = SchedulerConfig {
            logger: Some(logger.clone()),
            ..scheduler_config
        };

        let scheduler = Scheduler::new(adjusted_scheduler_config, task_placer, coordinator);
        let state = Arc::new(State {
            config,
            logger,
            scheduler,
            known_executors: RwLock::new(HashMap::new()),
            executor_metrics: RwLock::new(SampledMetrics::new()),
            concurrent_ticketings: Mutex::new(0),
        });

        Self { state }
    }

    /// Serves the GRPC backend. Waits for RegisterRequests from executors
    /// to starts scheduling tasks.
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        let scheduler_backend_server =
            protocol::scheduler_backend_server::SchedulerBackendServer::new(self.clone());

        let addr = self.state.config.bind_addr;

        let serve = tonic::transport::Server::builder()
            .add_service(scheduler_backend_server)
            .serve(addr);

        slog::info!(self.state.logger, "Serving"; "address" => addr);

        // Periodically log out metrics
        // TODO Join tasks with server

        let self1 = self.clone();
        tokio::spawn(async move {
            loop {
                task::sleep(Duration::from_secs(20)).await;
                let sampled_metrics = self1.state.executor_metrics.read().await;
                slog::info!(self1.state.logger, "Executor metrics"; "metrics" => &*sampled_metrics);
            }
        });

        // Periodicly pull new tasks from the scheduler
        // TODO Join tasks with server

        tokio::spawn(async move {
            loop {
                let num_executors = self.state.known_executors.read().await.len();

                let tasks_per_batch = 10;
                let task_duration = Duration::from_millis(
                    (tasks_per_batch as f32 * 100.0 / (5.0 * num_executors as f32)).floor() as u64,
                );

                task::sleep(task_duration).await;
                self.resource_offer().await;
            }
        });

        serve.await?;
        Ok(())
    }

    /// Beating heart of the GRPC backend: This function offers its known
    /// executors to the underlying scheduler to get to tasks which can
    /// be placed on the executors.
    async fn resource_offer(&self) {
        let offers: Vec<Offer<SocketAddr>> = self
            .state
            .known_executors
            .read()
            .await
            .keys()
            .map(|executor_id| Offer {
                executor_id: *executor_id,
                cores: 1,
                labels: vec![],
            })
            .collect();

        slog::debug!(self.state.logger, "Resource offer";
                     "num_executors" => offers.len()
        );

        let tickets = self.state.scheduler.resource_offer(&offers).await;

        let self1 = self.clone();

        // Making ticketing async will exploit more concurrency in the
        // the whole system. But in order to not overwhelm Tokios runtime
        // we better employ some upper bound on the number of concurrent
        // ticketings.

        // TODO use CondVar with next async_std release
        loop {
            let mut concurrent_ticketings = self.state.concurrent_ticketings.lock().await;

            // 20 is pretty arbitrary but probably a reasonable upper
            // bound.
            if *concurrent_ticketings < 20 {
                *concurrent_ticketings += 1;
                break;
            }
        }

        // We got some tickets, let's schedule ticketing asynchronously
        let _ = tokio::spawn(async move {
            let mut failed = false;

            let known_executors = self1.state.known_executors.read().await;

            for ticket in &tickets {
                if let Some(executor_state) = known_executors.get(&ticket.executor_id) {
                    slog::debug!(self1.state.logger, "Offering ticket";
                                 "batch_id" => ticket.batch_id,
                                 "executor" => ticket.executor_id
                    );

                    let response = executor_state
                        .client
                        // Clients are cheap to clone. No need for taking additional lock.
                        .clone()
                        .new_ticket(tonic::Request::new(protocol::NewTicketRequest {
                            batch_id: Some(protocol::BatchId {
                                id: ticket.batch_id,
                                scheduler: Some(self1.state.config.service_addr.into()),
                            }),
                        }))
                        .await
                        .map_err(|error| {
                            slog::warn!(self1.state.logger, "{}", error);
                            error
                        });

                    if response.is_err() {
                        failed = true;
                        break;
                    }
                } else {
                    println!("Not found!!!!");
                }
            }

            *self1.state.concurrent_ticketings.lock().await -= 1;

            // Ok, we failed. Let's fail le batch
            if failed {
                slog::warn!(self1.state.logger, "Failing batch");

                // Don't fail batches twice
                let mut failed_batches = HashSet::new();
                for ticket in tickets {
                    if !failed_batches.contains(&ticket.batch_id) {
                        failed_batches.insert(ticket.batch_id);
                        self1
                            .state
                            .scheduler
                            .fail_batch(ticket.batch_id, "Failing batch")
                            .await;
                    }
                }
            }
        });

        slog::debug!(self.state.logger, "Resource offer complete");
    }

    /// Removes an executor from the known executors. No tasks will be
    /// scheduled on that executor anymore.
    async fn remove_executor(&self, executor: SocketAddr) {
        slog::debug!(self.state.logger, "Removing executor";
                     "executor_id" => executor
        );

        self.state.known_executors.write().await.remove(&executor);
        self.state.scheduler.remove_executor(&executor).await;

        slog::debug!(self.state.logger, "Removing executor complete";
                     "executor_id" => executor
        );
    }
}

#[tonic::async_trait]
impl<TaskPlacer, Coordinator> protocol::scheduler_backend_server::SchedulerBackend
    for GrpcBackend<TaskPlacer, Coordinator>
where
    TaskPlacer: 'static + crate::scheduler::TaskPlacer,
    Coordinator: 'static + TaskCoordinator,
{
    async fn register(
        &self,
        request: tonic::Request<protocol::RegisterRequest>,
    ) -> Result<tonic::Response<protocol::Empty>, tonic::Status> {
        let executor_id = SocketAddr::try_from(
            request
                .into_inner()
                .executor_address
                .ok_or(Status::invalid_argument("executor_address not present"))?,
        )?;

        slog::debug!(self.state.logger, "Received RegisterRequest";
                     "executor_id" => executor_id
        );

        // TODO register should be idempotent;

        // Create a GRPC endpoint from a newly registered executor.
        let endpoint = Endpoint::from_shared(format!("http://{}", executor_id))
            .map_err(|_error| Status::invalid_argument("could not parse into endpoint"))?
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(2)));

        slog::debug!(self.state.logger, "Connecting to executor";
                     "executor_id" => executor_id
        );

        let channel = endpoint.connect().await.map_err(|error| {
            slog::error!(self.state.logger, "Could not establish connection";
                         "executor_id" => executor_id,
                         "error" => error.to_string()
            );
            Status::internal(format!("Could not establish connection: {}", error))
        })?;

        let mut client = protocol::executor_service_client::ExecutorServiceClient::new(channel);

        // Set up frequent status updates. The stream gives us nice capabilities
        // for monitoring the executor.

        let mut status_stream = client
            .status_update(protocol::StatusUpdateRequest {
                scheduler: Some(protocol::Address::from(self.state.config.service_addr)),
                interval_seconds: self.state.config.heartbeat_interval.as_secs(),
            })
            .await?
            .into_inner();

        let self1 = self.clone();

        tokio::spawn(async move {
            let mut prev_metrics = SampledMetrics::new();

            loop {
                match status_stream.message().await {
                    Ok(Some(status_response)) => {
                        slog::info!(self1.state.logger, "Received status";
                                    "Executor_id" => executor_id
                        );

                        let mut aggregated_metrics = self1.state.executor_metrics.write().await;
                        *aggregated_metrics -= prev_metrics.clone();
                        prev_metrics = SampledMetrics::from_status_update_response(status_response);
                        *aggregated_metrics += prev_metrics.clone();
                    }
                    Ok(None) => {
                        slog::info!(self1.state.logger, "Status stream ended";
                                    "Executor_id" => executor_id
                        );

                        // Stream has nothing more to send. Happens in graceful
                        // shutdown
                        self1.remove_executor(executor_id).await;
                        break;
                    }
                    Err(_) => {
                        slog::warn!(self1.state.logger, "Status stream lost peer";
                                    "Executor_id" => executor_id
                        );

                        // Connection terminated abnormally
                        self1.remove_executor(executor_id).await;
                        break;
                    }
                }
            }
        });

        slog::debug!(self.state.logger, "Connection established";
                     "executor_id" => executor_id
        );

        let executor_state = ExecutorState { client };

        self.state
            .known_executors
            .write()
            .await
            .insert(executor_id, executor_state);

        self.resource_offer().await;

        slog::debug!(self.state.logger, "RegisterRequest completed";
                     "executor_id" => executor_id
        );

        Ok(tonic::Response::new(protocol::Empty {}))
    }

    async fn complete_task(
        &self,
        request: tonic::Request<protocol::CompleteTaskRequest>,
    ) -> Result<tonic::Response<protocol::CompleteTaskResponse>, tonic::Status> {
        let complete_request = request.into_inner();

        let executor_id = SocketAddr::try_from(
            complete_request
                .executor_address
                .ok_or(Status::invalid_argument("executor_address not present"))?,
        )?;

        let request_task_id = complete_request
            .task_id
            .ok_or(Status::invalid_argument("task_id not present"))?;

        let task_id = TaskId {
            batch_id: request_task_id
                .batch_id
                .ok_or(Status::invalid_argument("No BatchId on TaskId"))?
                .id,
            local_id: request_task_id.local_id as usize,
        };

        slog::debug!(self.state.logger, "Received CompleteTaskRequest";
                     "executor_id" => executor_id,
                     "task_id" => task_id.local_id,
                     "batch_id" => task_id.batch_id
        );

        let request_result = complete_request
            .result
            .ok_or(Status::invalid_argument("result not present"))?;

        let result = match request_result {
            protocol::complete_task_request::Result::EncodedResult(bytes) => Ok(bytes),
            protocol::complete_task_request::Result::ErrorMessage(message) => {
                Err(FailedReason::ApplicationError(Some(message)))
            }
            protocol::complete_task_request::Result::DecodeTaskError(_) => {
                Err(FailedReason::DecodeTaskError)
            }
        };

        self.state
            .scheduler
            .complete_task(executor_id, task_id, result)
            .await;

        slog::debug!(self.state.logger, "CompleteTaskRequest complete";
                     "executor_id" => executor_id,
                     "task_id" => task_id.local_id,
                     "batch_id" => task_id.batch_id
        );

        if let Some(next_batch) = complete_request.next_redeem {
            slog::info!(self.state.logger, "Redeeming ticket in one go";
                        "executor_id" => executor_id,
                        "batch_id" => next_batch.id
            );

            let redeem_result = self
                .state
                .scheduler
                .redeem_ticket(executor_id, next_batch.id)
                .await;

            match redeem_result {
                Ok(launch_task) => Ok(tonic::Response::new(protocol::CompleteTaskResponse {
                    spawn_task: Some(protocol::SpawnTask {
                        task_id: Some(protocol::TaskId {
                            batch_id: Some(next_batch),
                            local_id: launch_task.task_id.local_id as u64,
                        }),
                        task: launch_task.encoded_task,
                    }),
                })),
                Err(RedeemTicketError::TicketExpired) => {
                    Ok(tonic::Response::new(protocol::CompleteTaskResponse {
                        spawn_task: None,
                    }))
                }
            }
        } else {
            Ok(tonic::Response::new(protocol::CompleteTaskResponse {
                spawn_task: None,
            }))
        }
    }

    async fn redeem_ticket(
        &self,
        request: tonic::Request<protocol::RedeemTicketRequest>,
    ) -> Result<tonic::Response<protocol::RedeemTicketResponse>, tonic::Status> {
        let redeem_request = request.into_inner();

        let executor_id = SocketAddr::try_from(
            redeem_request
                .executor_address
                .ok_or(Status::invalid_argument("executor_address not present"))?,
        )?;

        let batch_id = redeem_request
            .batch_id
            .ok_or(Status::invalid_argument("batch_id not present"))?
            .id;

        slog::debug!(self.state.logger, "Received RedeemTicketRequest";
                     "executor_id" => executor_id,
                     "batch_id" => batch_id
        );

        let redeem_result = self
            .state
            .scheduler
            .redeem_ticket(executor_id, batch_id)
            .await;

        slog::debug!(self.state.logger, "RedeemTicketRequest complete";
                     "executor_id" => executor_id,
                     "batch_id" => batch_id,
        );

        match redeem_result {
            Ok(launch_task) => Ok(tonic::Response::new(protocol::RedeemTicketResponse {
                redeem_response: Some(protocol::redeem_ticket_response::RedeemResponse::SpawnTask(
                    protocol::SpawnTask {
                        task_id: Some(protocol::TaskId {
                            batch_id: Some(protocol::BatchId {
                                id: batch_id,
                                scheduler: Some(self.state.config.service_addr.into()),
                            }),
                            local_id: launch_task.task_id.local_id as u64,
                        }),
                        task: launch_task.encoded_task,
                    },
                )),
            })),
            Err(RedeemTicketError::TicketExpired) => {
                Ok(tonic::Response::new(protocol::RedeemTicketResponse {
                    redeem_response: Some(
                        protocol::redeem_ticket_response::RedeemResponse::TicketExpired(
                            protocol::TicketExpired {},
                        ),
                    ),
                }))
            }
        }
    }
}
