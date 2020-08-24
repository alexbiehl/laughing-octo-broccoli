use red::grpc_backend::executor::Executor;
use red::grpc_backend::Config;
use red::grpc_backend::GrpcBackend;

use red::scheduler::task_coordinator::tests::InitialCoordinator;

use red::scheduler::task_coordinator::tests::MiddleMileRunner;

use red::scheduler::task_coordinator::TaskCoordinator;
use red::scheduler::task_placer::UnconstrainedTaskPlacer;
use red::scheduler::SchedulerConfig;
use red::task_runner::ForkScheduler;

use async_std::net::TcpListener;
use async_std::net::ToSocketAddrs;
use async_std::sync::Mutex;
use async_std::task;
use async_trait::async_trait;
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
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio;

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
            .expect("Could not bind to [::0]:0")
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
                .unwrap()
                .next()
                .unwrap(),
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
    let input_path = env::var("INPUT_PATH").expect("INPUT_PATH not set");
    let config_path = env::var("CONFIG_PATH").expect("CONFIG_PATH not set");

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
        .expect("Resolving failed")
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

    // let mut middlemile =
    //     MiddleMileCoordinator::new(logger.clone(), &run_id, &config_path, &input_path);

    // slog::info!(logger, "Running item fetch");

    // while let Some(item) = middlemile.receive_item.recv().await {
    //     slog::info!(logger, "Got one");
    // }

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

    match mode.as_str() {
        "scheduler" => {
            let _task_placer = UnconstrainedTaskPlacer::new();
            let task_coordinator = InitialCoordinator::with_num_items(logger.clone(), 2);
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

            let runner = MiddleMileRunner::new(logger.clone(), &run_id, &config_path, &input_path);

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
