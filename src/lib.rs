#![type_length_limit = "2717118"]

pub mod grpc_backend;
pub mod local_backend;
pub mod scheduler;
pub mod task_runner;

#[cfg(test)]
mod tests {

    use crate::grpc_backend::executor::Executor;
    use crate::grpc_backend::Config;
    use crate::grpc_backend::GrpcBackend;
    use crate::scheduler::task_coordinator::tests::InitialCoordinator;
    use crate::scheduler::task_coordinator::tests::TestCoordinator;
    use crate::scheduler::task_coordinator::TaskCoordinator;
    use crate::scheduler::task_placer::UnconstrainedTaskPlacer;
    use crate::scheduler::SchedulerConfig;
    use crate::task_runner::ForkScheduler;

    use async_std::net::TcpListener;
    use async_std::net::ToSocketAddrs;

    use async_trait::async_trait;
    use futures;
    use futures::future;
    use slog;
    use sloggers::terminal::Destination;
    use sloggers::terminal::TerminalLoggerBuilder;
    use sloggers::types::OverflowStrategy;
    use sloggers::types::Severity;
    use sloggers::Build;
    use std::boxed::Box;

    use std::error::Error;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio;
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct NewScheduler {
        logger: slog::Logger,
        register_new_scheduler: mpsc::UnboundedSender<SocketAddr>,
    }

    #[async_trait]
    impl ForkScheduler for NewScheduler {
        async fn fork<Coordinator>(
            &self,
            task_coordinator: Coordinator,
        ) -> Result<(), Box<dyn Error>>
        where
            Coordinator: 'static + TaskCoordinator,
        {
            let scheduler_config = SchedulerConfig {
                logger: Some(self.logger.clone()),
                ..SchedulerConfig::default()
            };

            let scheduler_port = allocate_port().await;
            let scheduler_address = ("::1", scheduler_port)
                .to_socket_addrs()
                .await
                .expect("Could not resolve socket")
                .next()
                .expect("No socket");
            let scheduler_bind = ("::0", scheduler_port)
                .to_socket_addrs()
                .await
                .expect("Could not resolve socket")
                .next()
                .expect("No socket");
            let task_placer = UnconstrainedTaskPlacer::new();
            let config = Config {
                service_addr: scheduler_address.clone(),
                bind_addr: scheduler_bind,
                heartbeat_interval: Duration::from_secs(10),
            };

            self.register_new_scheduler.send(scheduler_address)?;

            GrpcBackend::new(config, scheduler_config, task_placer, task_coordinator)
                .run()
                .await?;

            Ok(())
        }
    }

    async fn allocate_port() -> u16 {
        TcpListener::bind("[::]:0")
            .await
            .expect("Could not bind to [::1]:0")
            .local_addr()
            .expect("No local_addr in listener")
            .port()
    }

    //#[test]
    fn run_grpc() -> Result<(), Box<dyn std::error::Error>> {
        let mut rt = Runtime::new()?;

        rt.block_on(async {
            let mut builder = TerminalLoggerBuilder::new();
            builder.level(Severity::Trace);
            builder.destination(Destination::Stderr);
            builder.overflow_strategy(OverflowStrategy::Block);
            let logger = builder.build().unwrap();
            let logger1 = logger.clone();

            slog::info!(logger, "Initializing");

            let (send_new_scheduler, mut receive_scheduler) = mpsc::unbounded_channel();

            let new_scheduler = NewScheduler {
                logger: logger1.clone(),
                register_new_scheduler: send_new_scheduler.clone(),
            };

            let scheduler = async {
                new_scheduler
                    .fork(InitialCoordinator::new(logger1.clone()))
                    .await
                    .expect("Forking scheduler failed");
            };

            let executors = Arc::new(
                future::join_all((1..3).map(|_| async {
                    let port = allocate_port().await;
                    Executor::new(
                        logger1.clone(),
                        new_scheduler.clone(),
                        TestCoordinator::new(logger1.clone()),
                        ("::1", port)
                            .to_socket_addrs()
                            .await
                            .expect("Could not resolve socket")
                            .next()
                            .expect("No socket"),
                    )
                }))
                .await,
            );
            let executors1 = executors.clone();

            slog::info!(logger1, "Starting");

            async fn run_exec(executor: &Executor<NewScheduler, TestCoordinator>) {
                executor.run().await.expect("Error running executor");
            }

            let running_executors: Vec<_> = executors
                .iter()
                .map(|executor| run_exec(executor))
                .collect();

            let add_scheduler = async move {
                loop {
                    if let Some(scheduler) = receive_scheduler.recv().await {
                        future::join_all(executors1.iter().map(|executor| async move {
                            executor.add_scheduler(scheduler).await
                        }))
                        .await;
                    } else {
                        break;
                    }
                }
            };

            slog::info!(logger1, "Waiting");
            futures::join!(
                scheduler,
                add_scheduler,
                futures::future::join_all(running_executors)
            );
        });

        Ok(())
    }
}
