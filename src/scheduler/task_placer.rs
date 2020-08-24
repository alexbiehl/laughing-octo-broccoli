use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::hash::Hash;
use std::iter;

/// Status of a Task from the perspective of a Scheduler.
#[derive(Copy, Clone)]
pub enum Status {
    /// Task has been handed to the scheduler for further
    /// processing.
    Submitted,
    /// Scheduler sent out tickets for task.
    Ticketed,
    /// An executor redeemed a ticket and is now running
    /// task.
    Running,
    /// Scheduler received an explicit complete task signal.
    /// Task might have failed with an application specific
    /// error.
    Complete,
    /// Task got lost due to unreachable executor.
    Lost,
    /// Task got explicitly cancelled.
    Cancelled,
    /// Task was set to failure either due to infrastructure
    /// or scheduler issues.
    Failed,
}

impl Status {
    fn is_placeable(&self) -> bool {
        match self {
            Self::Submitted => true,
            Self::Ticketed => true,
            _ => false,
        }
    }

    fn is_completed(&self) -> bool {
        match self {
            Self::Complete => true,
            Self::Lost => true,
            Self::Cancelled => true,
            Self::Failed => true,
            _ => false,
        }
    }
}

/// An offer by the scheduler backend to the TaskPlacer.
pub struct Offer<ExecutorId> {
    /// Uniquely identifies an executor
    pub executor_id: ExecutorId,
    /// Number of cores to offer on the executor
    pub cores: u16,
    /// Labels of the executor
    pub labels: Vec<String>,
}

/// A local TaskId that is only valid within a
/// specific placement.
pub type LocalTaskId = usize;

/// Assignment of tasks to executors.
#[derive(Debug)]
pub struct Assignment {
    unconstrained: usize,
    constrained: LinkedList<LocalTaskId>,
}

/// A task attributed with some metadata.
pub struct ManagedTask<Task> {
    /// The underlying task
    real_task: Option<Task>,
    /// Status of the task
    status: Status,
}

impl<Task> ManagedTask<Task> {
    /// Creates a new ManagedTask in Ticketed state.
    pub fn new(task: Task) -> Self {
        ManagedTask {
            real_task: Some(task),
            status: Status::Ticketed,
        }
    }

    fn is_placeable(&self) -> bool {
        self.status.is_placeable()
    }

    fn is_completed(&self) -> bool {
        self.status.is_completed()
    }

    /// Setting a task to running. This moves the ManagedTask
    /// into running state and returns the underlying task if
    /// it wasn't running before.
    pub fn set_running(&mut self) -> Option<&mut Task> {
        if self.status.is_placeable() {
            self.status = Status::Running;
            let result = self.real_task.as_mut();
            assert!(result.is_some());
            result
        } else {
            None
        }
    }

    /// Setting a task to complete. This moves the ManagedTask
    /// into completed state and return the task if it hasn't
    /// been completed before. Subsequent access to the underlying
    /// task will fail.
    pub fn set_complete(&mut self) -> Option<Task> {
        if !self.status.is_completed() {
            self.status = Status::Complete;
            let result = std::mem::replace(&mut self.real_task, None);
            assert!(result.is_some());
            result
        } else {
            None
        }
    }

    /// Setting a task to lost. This moves the ManagedTask
    /// into completed state and return the task if it hasn't
    /// been completed before. Subsequent access to the underlying
    /// task will fail.
    pub fn set_lost(&mut self) -> Option<Task> {
        if !self.status.is_completed() {
            self.status = Status::Lost;
            let result = std::mem::replace(&mut self.real_task, None);
            assert!(result.is_some());
            result
        } else {
            None
        }
    }

    /// Setting a task to failed. This moves the ManagedTask
    /// into completed state and return the task if it hasn't
    /// been completed before. Subsequent access to the underlying
    /// task will fail.
    pub fn set_failed(&mut self) -> Option<Task> {
        if !self.status.is_completed() {
            self.status = Status::Failed;
            let result = std::mem::replace(&mut self.real_task, None);
            assert!(result.is_some());
            result
        } else {
            None
        }
    }
}

/// Decision of how to place tasks onto executors.
pub struct TaskSet<ExecutorId, Task> {
    /// Set of unconstrained tasks to schedule.
    unconstrained_tasks: Vec<ManagedTask<Task>>,
    /// Set of constrained tasks to scheduler.
    constrained_tasks: Vec<ManagedTask<Task>>,
    /// Assignment of Tasks to executors.
    assignment: HashMap<ExecutorId, Assignment>,
    /// Index to the next runnable task.
    next_runnable_task: usize,
    /// Number of tasks in this TaskSet that are still runnable
    remaining_tasks: usize,
}

impl<ExecutorId, Task> TaskSet<ExecutorId, Task> {
    pub fn tickets(&self) -> impl Iterator<Item = &ExecutorId> {
        self.assignment
            .iter()
            .flat_map(|(executor_id, assignment)| {
                iter::repeat(executor_id)
                    .take(assignment.unconstrained + assignment.constrained.len())
            })
    }

    pub fn managed_tasks(&mut self) -> impl Iterator<Item = &mut ManagedTask<Task>> {
        self.unconstrained_tasks
            .iter_mut()
            .chain(self.constrained_tasks.iter_mut())
    }

    pub fn task_ids<'a>(&'a self) -> impl Iterator<Item = LocalTaskId> + 'a {
        let unconstrained =
            (0..self.unconstrained_tasks.len()).map(move |i| self.to_unconstrained_local_id(i));
        let constrained = 0..self.constrained_tasks.len();

        constrained.chain(unconstrained)
    }

    // Decodes a LocalTaskId into the correct Task vector
    fn tasks(&mut self, local_id: LocalTaskId) -> &mut Vec<ManagedTask<Task>> {
        if local_id >= self.constrained_tasks.len() {
            &mut self.unconstrained_tasks
        } else {
            &mut self.constrained_tasks
        }
    }

    fn to_unconstrained_local_id(&self, local_id: usize) -> usize {
        self.constrained_tasks.len() + local_id
    }

    pub fn get_managed(&mut self, task_id: LocalTaskId) -> Option<&mut ManagedTask<Task>> {
        self.tasks(task_id).get_mut(task_id)
    }

    pub fn has_more_runnable_tasks(&self) -> bool {
        self.remaining_tasks <= 0
    }

    pub fn all_tasks_completed(&self) -> bool {
        fn is_complete<Task>(managed: &ManagedTask<Task>) -> bool {
            managed.is_completed()
        }

        !self.has_more_runnable_tasks()
            && self.unconstrained_tasks.iter().all(is_complete)
            && self.constrained_tasks.iter().all(is_complete)
    }

    /// Find the next runnable task for a given Executor. Give callers the chance
    /// modifiy the task at will as well.
    pub fn next_runnable_task(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Option<(LocalTaskId, &mut Task)>
    where
        ExecutorId: Hash + Eq,
    {
        if let Some(assignment) = self.assignment.get_mut(executor_id) {
            // Drop any non-placeable tasks up front
            while let Some(local_task_id) = assignment.constrained.front() {
                if let Some(task) = self.constrained_tasks.get(*local_task_id) {
                    if !task.is_placeable() {
                        assignment.constrained.pop_front();
                    }
                } else {
                    break;
                }
            }

            // Place constrained tasks first
            if let Some(local_task_id) = assignment.constrained.pop_front() {
                if let Some(managed_task) = self.constrained_tasks.get_mut(local_task_id) {
                    if let Some(task) = managed_task.set_running() {
                        self.remaining_tasks -= 1;
                        Some((local_task_id, task))
                    } else {
                        panic!("Invariant: Task is already running or completed")
                    }
                } else {
                    None
                }
            // No constrained tasks left, place unconstrained ones
            } else {
                // Drop the assignment for this executor if there's nothing
                // left to do
                if assignment.unconstrained > 1 {
                    assignment.unconstrained -= 1;
                } else {
                    self.assignment.remove(executor_id);
                }

                let local_task_id = self.next_runnable_task;
                let task_id = self.to_unconstrained_local_id(local_task_id);
                if let Some(managed_task) = self.unconstrained_tasks.get_mut(local_task_id) {
                    if let Some(task) = managed_task.set_running() {
                        self.next_runnable_task += 1;
                        self.remaining_tasks -= 1;
                        Some((task_id, task))
                    } else {
                        panic!("Invariant: Task is already running or completed")
                    }
                } else {
                    None
                }
            }
        } else {
            None
        }
    }
}

/// Decides where to places tasks.
#[async_trait]
pub trait TaskPlacer: Send + Sync {
    /// Place a batch of tasks onto a set of executors. It is the responsibility
    /// of the caller to make sure there each executor is only present once in
    /// the list of offers; otherwise results in undefined behavior.
    async fn place_tasks<ExecutorId, Task>(
        &mut self,
        tasks: Vec<Task>,
        executors: &[Offer<ExecutorId>],
    ) -> TaskSet<ExecutorId, Task>
    where
        Task: Send + Sync,
        ExecutorId: Send + Sync + Hash + Eq + Clone;

    /// Let the TaskPlacer know that an executor should not be considered
    /// for placement anymore. TODO: Is this necessary?
    async fn remove_executor<ExecutorId>(&self, _executor_id: ExecutorId)
    where
        ExecutorId: Send + Sync + Hash + Eq + Clone,
    {
    }
}

#[derive(Clone)]
pub struct UnconstrainedTaskPlacer {
    random_gen: StdRng,
}

impl UnconstrainedTaskPlacer {
    pub fn new() -> Self {
        let std_rng = SeedableRng::from_rng(thread_rng()).expect("StdRng not created");
        Self {
            random_gen: std_rng,
        }
    }
}

#[async_trait]
impl TaskPlacer for UnconstrainedTaskPlacer {
    /// Place a batch of tasks onto a set of executors.
    async fn place_tasks<ExecutorId, Task>(
        &mut self,
        input_tasks: Vec<Task>,
        executors: &[Offer<ExecutorId>],
    ) -> TaskSet<ExecutorId, Task>
    where
        ExecutorId: Hash + Eq + Send + Sync + Clone,
        Task: Send + Sync,
    {
        // Constant taken from Sparrow paper
        let unconstrained_probe_ratio: f32 = 1.05;
        let num_tickets = (input_tasks.len() as f32 * unconstrained_probe_ratio).ceil() as usize;
        let tickets_per_executor = num_tickets / executors.len();

        let assignment = executors
            .choose_multiple(&mut self.random_gen, num_tickets)
            .enumerate()
            .fold(HashMap::new(), |mut assignments, (i, offer)| {
                let assigned_num_tickets = if i < num_tickets % executors.len() {
                    tickets_per_executor + 1
                } else {
                    tickets_per_executor
                };

                // choose_multiple guarantees that each executor is only
                // drawn once.
                if let Some(_) = assignments.insert(
                    offer.executor_id.clone(),
                    Assignment {
                        unconstrained: assigned_num_tickets,
                        constrained: LinkedList::new(),
                    },
                ) {
                    panic!("Invariant: choose_multiple should never repeatedly draw same executor");
                }

                assignments
            });

        let remaining_tasks = input_tasks.len();

        TaskSet {
            unconstrained_tasks: input_tasks
                .into_iter()
                .map(|task| ManagedTask::new(task))
                .collect(),
            constrained_tasks: Vec::with_capacity(0),
            assignment,
            next_runnable_task: 0,
            remaining_tasks,
        }
    }
}
