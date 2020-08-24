use red::grpc_backend::executor::Executor;
use red::grpc_backend::Config;
use red::grpc_backend::GrpcBackend;

use red::scheduler::task_coordinator::Attempt;
use red::scheduler::task_coordinator::FailedReason;
use red::scheduler::task_coordinator::TaskCoordinator;
use red::scheduler::task_placer::UnconstrainedTaskPlacer;
use red::scheduler::SchedulerConfig;
use red::task_runner::ForkScheduler;
use red::task_runner::TaskRunner;

use async_std::net::TcpListener;
use async_std::net::ToSocketAddrs;
use async_std::sync::Mutex;
use async_std::task;
use async_trait::async_trait;
use bytes::BytesMut;
use prost::Message;
use redis;
use redis::AsyncCommands;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use sloggers::terminal::Destination;
use sloggers::terminal::TerminalLoggerBuilder;
use sloggers::types::OverflowStrategy;
use sloggers::types::Severity;
use sloggers::Build;
use std::boxed::Box;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::error::Error;

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

tonic::include_proto!("redpy.protocol");

struct Pyred {
    /// Handle to STDIN for incoming commands.
    input: Mutex<Box<dyn AsyncRead + Sync + Send + Unpin>>,
    /// Handle to STDOUT for outgoing signals.
    output: Mutex<Box<dyn AsyncWrite + Sync + Send + Unpin>>,
    /// Queue of tasks to be scheduled. In an Arc to allow external
    /// processes to schedule tasks concurrently.
    task_todo: Mutex<Vec<Attempt<<Self as TaskCoordinator>::Task>>>,
    /// Pending tasks
    pending_tasks: Mutex<HashMap<u64, oneshot::Sender<<Self as TaskRunner>::Result>>>,
}

impl Pyred {
    pub fn new() -> Self {
        Self {
            input: Mutex::new(Box::new(tokio::io::stdin())),
            output: Mutex::new(Box::new(tokio::io::stdout())),
            task_todo: Mutex::new(Vec::new()),
            pending_tasks: Mutex::new(HashMap::new()),
        }
    }

    async fn send_message<T: Message>(&self, message: T) {
        let message_size = message.encoded_len();

        // N.B. We'd rather go with &mut Vec to have smaller dependency
        // footprint but because of https://github.com/tokio-rs/bytes/issues/328
        // we have to use BytesMut
        let mut message_buffer = BytesMut::with_capacity(message_size);

        message
            .encode(&mut message_buffer)
            .expect("Encoding of message failed");

        let mut output = self.output.lock().await;

        // N.B. write_u64 writes a u64 in Big-Endian.
        output
            .write_u64(message_buffer.len() as u64)
            .await
            .expect("Writing of message length failed");
        output
            .write_all(&message_buffer)
            .await
            .expect("Writing of message failed");
        // Not flushing might leave stuff in buffers which
        // might clog processing on Python side
        output.flush().await.expect("Flushing failed");
    }

    async fn receive_message<T: Message + Default>(&self) -> Result<T, Box<dyn Error>> {
        let mut input = self.input.lock().await;

        // N.B. read_u64 reads a u64 in Big-Endian.
        let message_size = input.read_u64().await?;

        let mut message_buffer = vec![0; message_size as usize];

        input.read_exact(&mut message_buffer).await?;

        let result = prost::Message::decode(&message_buffer[..])?;
        Ok(result)
    }

    pub async fn run_input_listener(&self) -> Result<(), Box<dyn Error>> {
        loop {
            let input_message: Input = self.receive_message().await?;
            match input_message.input {
                Some(input::Input::SpawnTask(spawn_task)) => {
                    let attempt = Attempt::new(spawn_task);
                    // Cool, push the task onto the todo list and
                    // repeat
                    self.task_todo.lock().await.push(attempt);
                }
                Some(input::Input::TaskResult(task_result)) => {
                    if let Some(oneshot_sender) =
                        self.pending_tasks.lock().await.remove(&task_result.task_id)
                    {
                        oneshot_sender
                            .send(task_result)
                            .expect("Could not resolve task");
                    } else {
                        eprintln!("Zhopa")
                    }
                }
                None => {
                    // TODO: react appropriately to this very edge case
                    eprintln!("Zhopa");
                    break Ok(())
                }
            }
        }
    }
}

#[async_trait]
impl TaskRunner for Pyred {
    type Task = SpawnTask;

    type Result = TaskResult;

    fn decode_task(&self, input: Vec<u8>) -> Option<Self::Task> {
        prost::Message::decode(&input[..]).ok()
    }

    fn encode_result(&self, result: Self::Result) -> Vec<u8> {
        // N.B. We'd rather go with &mut Vec to have smaller dependency
        // footprint but because of https://github.com/tokio-rs/bytes/issues/328
        // we have to use BytesMut
        let mut buffer = BytesMut::with_capacity(result.encoded_len());
        result.encode(&mut buffer).expect("Could not encode result");
        buffer.to_vec()
    }

    async fn run_task<Fork>(
        &self,
        _fork: &Fork,
        task: Self::Task,
    ) -> Result<Self::Result, Box<dyn Error>>
    where
        Fork: ForkScheduler,
    {
        let task_id = task.task_id;
        let (sender, receiver) = oneshot::channel();
        self.pending_tasks.lock().await.insert(task_id, sender);

        self.send_message(Output {
            output: Some(output::Output::SpawnTask(task)),
        })
        .await;

        let result = receiver.await?;
        Ok(result)
    }
}

#[async_trait]
impl TaskCoordinator for Pyred {
    type Task = SpawnTask;

    type Result = TaskResult;

    fn encode_task(&self, task: &Self::Task) -> Vec<u8> {
        // N.B. We'd rather go with &mut Vec to have smaller dependency
        // footprint but because of https://github.com/tokio-rs/bytes/issues/328
        // we have to use BytesMut
        let mut buffer = BytesMut::with_capacity(task.encoded_len());
        task.encode(&mut buffer).expect("Could not encode task");
        buffer.to_vec()
    }

    fn decode_result(&self, input: Vec<u8>) -> Option<Self::Result> {
        prost::Message::decode(&input[..]).ok()
    }

    async fn resource_offer(&self) -> Vec<Attempt<Self::Task>> {
        let mut task_todo = self.task_todo.lock().await;
        std::mem::replace(&mut *task_todo, Vec::new())
    }

    async fn task_successful(&self, _attempt: Attempt<Self::Task>, result: Self::Result) {
        eprintln!("Geil");

        let output_message = Output {
            output: Some(output::Output::TaskResult(result)),
        };
        self.send_message(output_message).await;
    }

    async fn task_failed(&self, attempt: Attempt<Self::Task>, reason: FailedReason) {
        eprintln!("Krass");
        let task = attempt.into_inner();

        let output_message = Output {
            output: Some(output::Output::TaskResult(TaskResult {
                task_id: task.task_id,
                result: Some(task_result::Result::ErrorDescription(reason.to_string())),
            })),
        };
        self.send_message(output_message).await;
    }
}

#[derive(Clone)]
struct Forker {
    logger: slog::Logger,
    host: String,
    run_id: String,
    redis_connection_info: ConnectionInfo,
}

#[async_trait]
impl ForkScheduler for Forker {
    async fn fork<Coordinator>(&self, task_coordinator: Coordinator) -> Result<(), Box<dyn Error>>
    where
        Coordinator: 'static + TaskCoordinator,
    {
        // Bind a random port
        let port = TcpListener::bind("[::0]:0")
            .await
            .expect("Could not bind to [::0]")
            .local_addr()
            .expect("No local_addr in listener")
            .port();

        let resolved_address = (self.host.as_str(), port)
            .to_socket_addrs()
            .await
            .expect("Resolving failed")
            .next()
            .expect("Resolving socket failed");

        let external_address = format!("{}", resolved_address);
        // Set up the dance

        let scheduler_config = SchedulerConfig {
            logger: Some(self.logger.clone()),
            ..SchedulerConfig::default()
        };

        let task_placer = UnconstrainedTaskPlacer::new();

        let config = Config {
            service_addr: resolved_address,
            bind_addr: ("::0", resolved_address.port())
                .to_socket_addrs()
                .await
                .expect("Resolving failed")
                .next()
                .expect("Resolving socket failed"),
            heartbeat_interval: Duration::from_secs(10),
        };

        let connection_info = self.redis_connection_info.clone();
        let schedulers_key = format!("{}::schedulers", self.run_id);

        let signal = Arc::new(Mutex::new(false));
        let signal1 = signal.clone();

        tokio::spawn(async move {
            let mut redis_connection = redis::aio::connect(&connection_info)
                .await
                .expect("Connection to Redis failed");

            loop {
                if *signal.lock().await {
                    break;
                }

                let now: u64 = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|duration| duration.as_secs())
                    .expect("Could not get current timestamp since UNIX_EPOCH");

                let () = redis_connection
                    .zadd(&schedulers_key, &external_address, now + 60)
                    .await
                    .expect("Writing to redis failed");

                let () = redis_connection
                    .expire(&schedulers_key, Duration::from_secs(60).as_secs() as usize)
                    .await
                    .expect("Writing to Redis failed");

                task::sleep(Duration::from_secs(10)).await;
            }
        });

        GrpcBackend::new(config, scheduler_config, task_placer, task_coordinator)
            .run()
            .await?;

        *signal1.lock().await = true;

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    args.next(); // Drop app name
    let mode: String = args
        .next()
        .expect("Mode must be either scheduler or executor");

    let run_id = env::var("RUN_ID").expect("RUN_ID not set");
    let redis_host = env::var("REDIS_HOST").expect("REDIS_HOST not set");
    let redis_port = env::var("REDIS_PORT")
        .expect("REDIS_PORT not set")
        .parse()
        .expect("REDIS_PORT not an u16");
    let redis_db = env::var("REDIS_DB")
        .expect("REDIS_DB not set")
        .parse()
        .expect("REDIS_DB not an u8");
    let redis_password = match env::var("REDIS_PASSWORD") {
        Ok(redis_password) => Some(redis_password),
        Err(_) => None,
    };

    // Bind a random port
    let port = TcpListener::bind("[::0]:0")
        .await
        .expect("Could not bind to [::0]:0")
        .local_addr()
        .expect("No local_addr in listener")
        .port();

    let nm_host = env::var("NM_HOST").expect("NM_HOST environment variable not set");

    let resolved_address = (nm_host.as_str(), port)
        .to_socket_addrs()
        .await
        .expect("Resolving NM_HOST failed")
        .next()
        .expect("Resolving NM_HOST failed");

    let _external_address = format!("{}", resolved_address);

    let schedulers_key = format!("{}::schedulers", run_id);

    // Create a SocketAddr from NM_HOST and our random port

    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);
    builder.overflow_strategy(OverflowStrategy::Block);
    let logger = builder
        .build()
        .unwrap()
        .new(slog::o!("run_id" => run_id.clone()));

    // Set up the dance

    let new_scheduler = Forker {
        run_id: run_id.clone(),
        host: nm_host,
        logger: logger.clone(),
        redis_connection_info: ConnectionInfo {
            addr: Box::new(ConnectionAddr::Tcp(redis_host.clone(), redis_port)),
            db: redis_db,
            passwd: redis_password.clone(),
        },
    };

    let pyred = Arc::new(Pyred::new());
    let pyred1 = pyred.clone();

    tokio::spawn(async move {
        pyred1
            .run_input_listener()
            .await
            .expect("Error when listening for inputs");
    });

    match mode.as_str() {
        "scheduler" => {
            let _task_placer = UnconstrainedTaskPlacer::new();
            let task_coordinator = pyred;
            new_scheduler
                .fork(task_coordinator)
                .await
                .expect("Running root scheduler failed");
        }
        "executor" => {
            let logger = logger.new(slog::o!("executor" => resolved_address));

            let mut redis_connection = redis::aio::connect(&ConnectionInfo {
                addr: Box::new(ConnectionAddr::Tcp(redis_host, redis_port)),
                db: redis_db,
                passwd: redis_password,
            })
            .await
            .expect("Connection to Redis failed");

            let runner = pyred;

            let executor = Arc::new(Executor::new(
                logger.clone(),
                new_scheduler,
                runner,
                resolved_address,
            ));

            let executor1 = executor.clone();

            tokio::spawn(async move {
                let mut known_schedulers = HashSet::new();

                loop {
                    let now: u64 = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|duration| duration.as_secs())
                        .expect("Could not get current timestamp since UNIX_EPOCH");

                    let schedulers: Vec<String> = redis_connection
                        .zrevrangebyscore(&schedulers_key, std::f32::INFINITY, now)
                        .await
                        .expect("Could not read from redis");

                    for scheduler in schedulers {
                        if !known_schedulers.contains(&scheduler) {
                            known_schedulers.insert(scheduler.clone());

                            slog::info!(logger, "Adding scheduler"; "scheduler" => &scheduler);

                            let scheduler_addr = scheduler
                                .to_socket_addrs()
                                .await
                                .expect("Could not parse scheduler address")
                                .next()
                                .expect("Scheduler address not parseable");

                            executor1
                                .add_scheduler(scheduler_addr)
                                .await
                                .expect("Could not add scheduler");
                        }
                    }

                    task::sleep(Duration::from_secs(10)).await;
                }
            });

            executor.run().await.expect("Failed to run executor");
        }
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn encode_decode_task() {
        let pyred = Pyred::new();
        let task = SpawnTask {
            task_id: 1,
            serialized_task: vec![
                'h' as u8, 'e' as u8, 'l' as u8, 'l' as u8, 'o' as u8, 'h' as u8, 'e' as u8,
                'l' as u8, 'l' as u8, 'o' as u8,
            ],
        };

        assert!(pyred.decode_task(pyred.encode_task(&task)) == Some(task))
    }
}
