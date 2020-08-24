use crate::scheduler;
use crate::scheduler::FailedReason;
use crate::scheduler::Offer;
use crate::scheduler::RedeemTicketError;
use crate::scheduler::Scheduler;
use crate::scheduler::SchedulerConfig;
use crate::scheduler::TaskCoordinator;
use crate::scheduler::Ticket;
use crate::task_runner::ForkScheduler;

use async_std::stream::Stream;
use async_std::stream::StreamExt;
use async_std::sync::Mutex;
use async_std::task;
use async_trait::async_trait;
use futures::future;
use slog;
use std::error::Error;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct LocalExecutor<TaskRunner, TaskPlacer, Coordinator>
where
    TaskPlacer: crate::scheduler::TaskPlacer,
    Coordinator: TaskCoordinator,
{
    state: Arc<State<TaskRunner, TaskPlacer, Coordinator>>,
}

/// Eeh, notice the fix point? This sucks but well, it's the price to
/// pay for our three major component abstractions Executor, Placer and
/// Coordinator.
struct State<TaskRunner, TaskPlacer, Coordinator>
where
    TaskPlacer: crate::scheduler::TaskPlacer,
    Coordinator: TaskCoordinator,
{
    logger: slog::Logger,
    task_runner: Arc<TaskRunner>,
    scheduler:
        Scheduler<LocalExecutor<TaskRunner, TaskPlacer, Coordinator>, TaskPlacer, Coordinator>,
    forker: Forker<TaskPlacer, TaskRunner>,
}

impl<TaskRunner, TaskPlacer, Coordinator> scheduler::Executor
    for LocalExecutor<TaskRunner, TaskPlacer, Coordinator>
where
    TaskPlacer: crate::scheduler::TaskPlacer,
    Coordinator: TaskCoordinator,
{
    type Id = usize;
}

/// Internal type that represents executors.
struct Executor {
    join_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::UnboundedSender<Ticket<usize>>,
}

#[derive(Clone)]
struct Forker<TaskPlacer, TaskRunner> {
    scheduler_config: SchedulerConfig,
    task_placer: TaskPlacer,
    task_runner: Arc<TaskRunner>,
}

#[async_trait]
impl<TaskPlacer, TaskRunner> ForkScheduler for Forker<TaskPlacer, TaskRunner>
where
    TaskPlacer: 'static + crate::scheduler::TaskPlacer + Clone,
    TaskRunner: 'static + crate::task_runner::TaskRunner,
{
    async fn fork<Coordinator>(&self, task_coordinator: Coordinator) -> Result<(), Box<dyn Error>>
    where
        Coordinator: 'static + TaskCoordinator,
    {
        let local_executor = LocalExecutor::from_arc(
            self.scheduler_config.clone(),
            self.task_runner.clone(),
            self.task_placer.clone(),
            task_coordinator,
        );

        local_executor.run().await;
        Ok(())
    }
}

impl<TaskRunner, TaskPlacer, Coordinator> LocalExecutor<TaskRunner, TaskPlacer, Coordinator>
where
    TaskRunner: 'static + crate::task_runner::TaskRunner,
    TaskPlacer: 'static + crate::scheduler::TaskPlacer + Clone,
    Coordinator: 'static + TaskCoordinator,
{
    fn from_arc(
        scheduler_config: SchedulerConfig,
        task_runner: Arc<TaskRunner>,
        task_placer: TaskPlacer,
        coordinator: Coordinator,
    ) -> Self
    where
        TaskPlacer: Clone,
    {
        let logger = scheduler_config.get_logger();
        let scheduler = Scheduler::new(scheduler_config.clone(), task_placer.clone(), coordinator);

        Self {
            state: Arc::new(State {
                logger,
                task_runner: task_runner.clone(),
                scheduler,
                forker: Forker {
                    scheduler_config,
                    task_placer,
                    task_runner,
                },
            }),
        }
    }

    pub fn new(
        scheduler_config: SchedulerConfig,
        task_runner: TaskRunner,
        task_placer: TaskPlacer,
        coordinator: Coordinator,
    ) -> Self
    where
        TaskPlacer: Clone,
    {
        Self::from_arc(
            scheduler_config,
            Arc::new(task_runner),
            task_placer,
            coordinator,
        )
    }

    async fn executor_loop(
        state: Arc<State<TaskRunner, TaskPlacer, Coordinator>>,
        idle_counter: Arc<Mutex<usize>>,
        mut incoming: impl Stream<Item = Ticket<usize>> + std::marker::Unpin,
    ) {
        loop {
            *idle_counter.lock().await += 1;
            let next_ticket = incoming.next().await;

            if let None = next_ticket {
                break;
            }
            *idle_counter.lock().await -= 1;

            let ticket = next_ticket.expect("Ticket expected to be present");

            let redeem_result = state
                .scheduler
                .redeem_ticket(ticket.executor_id, ticket.batch_id)
                .await;
            match redeem_result {
                Ok(launch_task) => {
                    let task = state
                        .task_runner
                        .decode_task(launch_task.encoded_task)
                        .expect("Could not decode task");

                    let result = state
                        .task_runner
                        .run_task(&state.forker, task)
                        .await
                        .map(|result| state.task_runner.encode_result(result))
                        .map_err(|error| FailedReason::ApplicationError(Some(error.to_string())));

                    state
                        .scheduler
                        .complete_task(ticket.executor_id, launch_task.task_id, result)
                        .await;
                }
                Err(RedeemTicketError::TicketExpired {}) => {
                    slog::info!(state.logger, "Ticket expired");
                }
            }
        }
    }

    async fn spawn_executor(
        &self,
        idle_counter: Arc<Mutex<usize>>,
        _executor_id: usize,
    ) -> Executor {
        let (sender, receiver) = mpsc::unbounded_channel();
        let state = self.state.clone();
        let join_handle = tokio::spawn(async move {
            Self::executor_loop(state, idle_counter, receiver).await;
        });
        Executor {
            join_handle,
            sender,
        }
    }

    pub async fn run(&self) {
        let idle_counter = Arc::new(Mutex::new(0 as usize));

        let num_executors: usize = 8;
        let executors: Vec<_> = future::join_all(
            iter::repeat(idle_counter.clone())
                .enumerate()
                .take(num_executors)
                .map(|(executor_id, idle_counter)| async move {
                    self.spawn_executor(idle_counter, executor_id).await
                }),
        )
        .await;

        let offers: Vec<Offer<usize>> = (1..num_executors)
            .into_iter()
            .map(|executor_id| Offer {
                executor_id,
                cores: 1,
                labels: Vec::new(),
            })
            .collect();

        slog::info!(self.state.logger, "Starting local executor");

        loop {
            let tickets: Vec<_> = self.state.scheduler.resource_offer(&offers).await;

            if tickets.is_empty() {
                if *idle_counter.lock().await == num_executors {
                    break;
                }
            }

            for ticket in tickets {
                executors
                    .get(ticket.executor_id - 1)
                    .expect("Internal: Invalid executor_id")
                    .sender
                    .send(ticket)
                    .expect("Could not send ticket to executor");
            }

            task::sleep(Duration::from_secs(1)).await;
        }

        future::join_all(executors.into_iter().map(|executor| executor.join_handle)).await;
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::scheduler::task_coordinator::tests::InitialCoordinator;
    use crate::scheduler::task_coordinator::tests::TestCoordinator;
    use crate::scheduler::task_placer::UnconstrainedTaskPlacer;

    use sloggers::terminal::Destination;
    use sloggers::terminal::TerminalLoggerBuilder;
    use sloggers::types::OverflowStrategy;
    use sloggers::types::Severity;
    use sloggers::Build;
    use tokio::runtime::Runtime;

    #[test]
    fn run_local() -> Result<(), Box<dyn std::error::Error>> {
        let mut rt = Runtime::new()?;

        rt.block_on(async {
            let mut builder = TerminalLoggerBuilder::new();
            builder.level(Severity::Trace);
            builder.destination(Destination::Stderr);
            builder.overflow_strategy(OverflowStrategy::Block);
            let logger = builder.build().unwrap();

            let task_placer = UnconstrainedTaskPlacer::new();
            let task_coordinator = InitialCoordinator::new(logger.clone());
            let task_runner = TestCoordinator::new(logger.clone());

            let local_executor = LocalExecutor::new(
                SchedulerConfig {
                    logger: Some(logger),
                    ..SchedulerConfig::default()
                },
                task_runner,
                task_placer,
                task_coordinator,
            );

            local_executor.run().await;
            Ok(())
        })
    }
}
