pub mod executor;
pub mod task_coordinator;
pub mod task_placer;

pub use executor::Executor;
pub use task_coordinator::Attempt;
pub use task_coordinator::FailedReason;
pub use task_coordinator::TaskCoordinator;
pub use task_placer::Offer;
pub use task_placer::Status;
pub use task_placer::TaskPlacer;
pub use task_placer::TaskSet;

use async_std::sync::Mutex;
use async_std::sync::MutexGuard;
use futures::future;
use slog;
use slog::Logger;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Scheduler unique identifier for a Task.
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct TaskId {
    /// Opaque, batch local task id
    pub local_id: usize,
    pub batch_id: BatchId,
}

/// Uniquely identifes a Batch in a Scheduler.
type BatchId = u64;

/// Tickets are being sent to executors.
#[derive(Clone, Copy, Debug)]
pub struct Ticket<ExecutorId> {
    /// Ticket assigned to this executor
    pub executor_id: ExecutorId,
    /// Uniquely identifies a set of tasks in the scheduler
    pub batch_id: BatchId,
    /// The priority with which to schedule the ticket
    pub priority: u16,
}

/// Parameters for launching a task on an executor.
pub struct LaunchTask {
    /// TaskId of the task at hand
    pub task_id: TaskId,
    /// Encoded bytes of the task
    pub encoded_task: Vec<u8>,
}

/// An executor updates the status of a task.
pub struct StatusUpdate {
    /// TaskId for the Task to update status for
    pub task_id: TaskId,
    /// New status
    pub status: Status,
}

/// Error returned when redeeming a ticket.
pub enum RedeemTicketError {
    TicketExpired,
}

/// General configuration properties determining Scheduler
/// behavior.
#[derive(Clone)]
pub struct SchedulerConfig {
    pub logger: Option<Logger>,
    pub concurrency: u16,
    pub priority: u16,
}

impl SchedulerConfig {
    pub fn default() -> Self {
        SchedulerConfig {
            concurrency: 8,
            priority: 0,
            logger: None,
        }
    }

    pub fn get_logger(&self) -> Logger {
        self.logger
            .as_ref()
            .map(|logger| logger.clone())
            .unwrap_or(Logger::root(slog::Discard, slog::o!()))
    }
}

/// The core piece of the of the project. A Scheduler is parameterized
/// over the executor and coordinator to make sure to keep the boundaries
/// well defined.
pub struct Scheduler<Executor, TaskPlacer, Coordinator>
where
    Executor: executor::Executor,
    Coordinator: TaskCoordinator,
{
    /// Scheduler configuration,
    config: SchedulerConfig,
    /// Logger used for the scheduler,
    logger: Logger,
    /// High-level TaskCoordinator
    task_coordinator: Coordinator,
    /// Unique id generator
    next_batch_id: AtomicU64,
    /// Shared, internal state of the scheduler
    shards: Vec<Mutex<Shard<Executor::Id, TaskPlacer, Coordinator::Task>>>,
}

/// Internal state of the Scheduler. It is guarded by locks.
struct Shard<ExecutorId, TaskPlacer, Task> {
    /// Task placer
    task_placer: TaskPlacer,
    /// Mapping from TaskId to ExecutorId
    task_to_executor: HashMap<TaskId, ExecutorId>,
    /// Mapping from ExecutorId to TaskId
    executor_to_task: HashMap<ExecutorId, HashSet<TaskId>>,
    /// The in-flight scheduled tasks
    task_sets: HashMap<BatchId, TaskSet<ExecutorId, Attempt<Task>>>,
}

impl<Executor, TaskPlacer, Coordinator> Scheduler<Executor, TaskPlacer, Coordinator>
where
    Coordinator: TaskCoordinator,
    TaskPlacer: task_placer::TaskPlacer,
    Executor: executor::Executor,
    Executor::Id: Hash + Eq + Send + Sync + Clone,
{
    /// Create a new `Scheduler`. This function is very cheap.
    pub fn new(
        config: SchedulerConfig,
        task_placer: TaskPlacer,
        task_coordinator: Coordinator,
    ) -> Self
    where
        TaskPlacer: Clone,
    {
        let shards = (1..config.concurrency)
            .into_iter()
            .map(|_| {
                Mutex::new(Shard {
                    task_to_executor: HashMap::new(),
                    executor_to_task: HashMap::new(),
                    task_sets: HashMap::new(),
                    task_placer: task_placer.clone(),
                })
            })
            .collect();

        let logger = config.get_logger();

        Self {
            config,
            logger,
            task_coordinator,
            next_batch_id: AtomicU64::new(1),
            shards,
        }
    }

    pub fn get_logger(&self) -> &Logger {
        &self.logger
    }

    /// Internal function to shard the work between batches onto multiple, concurrent
    /// Schedulers.
    async fn with_shard(
        &self,
        batch_id: BatchId,
    ) -> MutexGuard<'_, Shard<Executor::Id, TaskPlacer, Coordinator::Task>> {
        let num_shards = self.shards.len();
        let locked_shard = self
            .shards
            .get(batch_id as usize % num_shards)
            .expect("Internal: invalid index");
        locked_shard.lock().await
    }

    /// Returns a new, unique BatchId
    fn next_batch_id(&self) -> BatchId {
        // Relaxed memory ordering is fine as we only want one
        // atomic operation
        self.next_batch_id.fetch_add(1, Ordering::Relaxed)
    }

    // Offer resources to the scheduler. The scheduler places tasks
    // on the resources if it can.
    // Returns an iterator over the tickets to send to executors -
    // this "streaming" design allows us to not allocate too much.
    pub async fn resource_offer(
        &self,
        offers: &[Offer<Executor::Id>],
    ) -> Vec<Ticket<Executor::Id>> {
        slog::debug!(self.logger, "Resource offer");

        // TODO: Ugh early exit not very functional
        if offers.is_empty() {
            return vec![];
        }

        // Generate a unique identifier for this batch of tasks
        let batch_id = self.next_batch_id();

        // Let's ask the coordinator first wether it got tasks to
        // schedule.
        let tasks_todo = self.task_coordinator.resource_offer().await;

        let mut shard = self.with_shard(batch_id).await;

        // Place the tasks onto the offers
        let task_set: TaskSet<Executor::Id, Attempt<Coordinator::Task>> =
            shard.task_placer.place_tasks(tasks_todo, offers).await;

        // Insert task_set into our set of live tasks and return
        // a reference to it. task_set is now owned by the Scheduler.
        // We may return references to it now.
        let owned_task_set: &TaskSet<Executor::Id, Attempt<Coordinator::Task>> =
            match shard.task_sets.entry(batch_id) {
                Entry::Vacant(entry) => entry.insert(task_set),
                Entry::Occupied(_entry) => panic!("batch_ids supposed to be unique!"),
            };

        let priority = self.config.priority;

        let tickets: Vec<Ticket<Executor::Id>> = owned_task_set
            .tickets()
            .map(|executor_id| Ticket {
                executor_id: executor_id.clone(),
                batch_id,
                priority,
            })
            .collect();

        slog::debug!(self.logger, "Resource offer complete";
                     "num_tickets" => tickets.len()
        );

        tickets
    }

    pub async fn redeem_ticket(
        &self,
        executor_id: Executor::Id,
        batch_id: BatchId,
    ) -> Result<LaunchTask, RedeemTicketError> {
        slog::debug!(self.logger, "Redeem ticket for batch"; "batch_id" => batch_id);

        let mut shard = self.with_shard(batch_id).await;

        if let Some(task_set) = shard.task_sets.get_mut(&batch_id) {
            if let Some((local_task_id, attempt)) = task_set.next_runnable_task(&executor_id) {
                let task_id = TaskId {
                    local_id: local_task_id,
                    batch_id,
                };

                let launch_task = LaunchTask {
                    task_id,
                    encoded_task: self.task_coordinator.encode_task(&attempt.task),
                };

                slog::debug!(self.logger, "Spawning task"; "task_id" => task_id.local_id, "batch_id" => batch_id);

                shard
                    .executor_to_task
                    .entry(executor_id.clone())
                    .and_modify(|tasks| {
                        tasks.insert(task_id);
                    })
                    .or_insert_with(|| {
                        let mut tasks = HashSet::new();
                        tasks.insert(task_id);
                        tasks
                    });

                shard.task_to_executor.insert(task_id, executor_id);
                Ok(launch_task)
            } else {
                Err(RedeemTicketError::TicketExpired)
            }
        } else {
            Err(RedeemTicketError::TicketExpired)
        }
    }

    pub async fn complete_task(
        &self,
        executor_id: Executor::Id,
        task_id: TaskId,
        result: Result<Vec<u8>, FailedReason>,
    ) {
        let mut shard = self.with_shard(task_id.batch_id).await;

        if let Some(task_set) = shard.task_sets.get_mut(&task_id.batch_id) {
            if let Some(managed_task) = task_set.get_managed(task_id.local_id) {
                if let Some(attempt) = managed_task.set_complete() {
                    match result {
                        Ok(result_bytes) => {
                            if let Some(result) = self.task_coordinator.decode_result(result_bytes)
                            {
                                self.task_coordinator.task_successful(attempt, result).await;
                            } else {
                                self.task_coordinator
                                    .task_failed(attempt, FailedReason::DecodeResultError)
                                    .await;
                            }
                        }
                        Err(error) => self.task_coordinator.task_failed(attempt, error).await,
                    }
                } else {
                    // TODO task was already completed
                    ()
                }
            } else {
                // TODO We received a completion for a Task that doesn't exists
                // in this batch.
                ()
            }

            // Once every task is completed, we can remove the batch from
            // the scheduler.
            if task_set.all_tasks_completed() {
                shard.task_sets.remove(&task_id.batch_id);
            }

            // Remove reference to this task from the global mapping
            match shard.executor_to_task.entry(executor_id) {
                Entry::Occupied(mut occupied) => {
                    let executor_tasks = occupied.get_mut();
                    executor_tasks.remove(&task_id);
                    if executor_tasks.is_empty() {
                        // Make sure to not keep empty sets around
                        occupied.remove_entry();
                    }
                }
                Entry::Vacant(_) => {}
            }

            shard.task_to_executor.remove(&task_id);
        } else {
            // TODO We received a completion for a batch that is long gone(?)
            ()
        }
    }

    /// Fails all in-flight tasks in a given Batch.
    pub async fn fail_batch(&self, batch_id: BatchId, _error_msg: &str) {
        let mut shard = self.with_shard(batch_id).await;
        if let Some(task_set) = shard.task_sets.get_mut(&batch_id) {
            slog::debug!(self.logger, "Failing batch";
                        "batch_id" => batch_id
            );
            for managed_task in task_set.managed_tasks() {
                if let Some(attempt) = managed_task.set_failed() {
                    self.task_coordinator
                        .task_failed(attempt, FailedReason::LostExecutor)
                        .await;
                } else {
                    // TODO task was already completed
                }
            }

            if task_set.all_tasks_completed() {
                shard.task_sets.remove(&batch_id);
            }
        } else {
            slog::warn!(self.logger, "Trying to fail non-existent batch";
                        "batch_id" => batch_id
            );
        }
    }

    pub async fn remove_executor(&self, executor_id: &Executor::Id) {
        future::join_all(self.shards.iter().map(|locked_shard| async move {
            let mut shard = locked_shard.lock().await;

            if let Some(task_ids) = shard.executor_to_task.remove(executor_id) {
                for task_id in task_ids {
                    if let Some(task_set) = shard.task_sets.get_mut(&task_id.batch_id) {
                        if let Some(managed_task) = task_set.get_managed(task_id.local_id) {
                            if let Some(attempt) = managed_task.set_lost() {
                                self.task_coordinator
                                    .task_failed(attempt, FailedReason::LostExecutor)
                                    .await;
                            } else {
                                // TODO Task was already completed
                            }
                        }

                        // Once every task is completed, we can remove the batch from
                        // the scheduler.
                        if task_set.all_tasks_completed() {
                            shard.task_sets.remove(&task_id.batch_id);
                        }
                    }
                }
            }
        }))
        .await;
    }

    pub fn status_update(&self, _executor_id: Executor::Id, _task_id: TaskId) {
        // TODO care about dieing and lost tasks
    }
}
