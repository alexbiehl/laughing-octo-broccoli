use async_trait::async_trait;
use std::boxed::Box;
use std::ops::Deref;

/// Reason why a task has failed.
#[derive(Clone, Debug)]
pub enum FailedReason {
    /// Indicates an error due to a lost executor.
    LostExecutor,
    /// Decoding of a task has failed. This might be
    /// due to a mismatch between scheduler and executor
    /// code.
    DecodeTaskError,
    /// Decoding of a result has failed. This might be
    /// due to a mismatch between scheduler and executor
    /// code.
    DecodeResultError,
    /// The task failed due to an application error. Might
    /// contain a textual description of the error.
    ApplicationError(Option<String>),
}

impl std::fmt::Display for FailedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::LostExecutor => write!(f, "{}", "lost executor"),
            Self::DecodeTaskError => write!(f, "{}", "decoding task failed"),
            Self::DecodeResultError => write!(f, "{}", "decoding result failed"),
            Self::ApplicationError(Some(err)) => {
                write!(f, "{}: {}", "application specific error", err)
            }
            Self::ApplicationError(None) => write!(f, "{}", "application specific error"),
        }
    }
}

impl FailedReason {
    /// Should the error be treated as application specific or
    /// does it indicate a scheduler/infrastructure issue.
    pub fn is_application_specific_error(&self) -> bool {
        match self {
            FailedReason::ApplicationError(_) => true,
            _ => false,
        }
    }
}

/// Task attempt.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Attempt<Task> {
    /// Attempted task
    pub task: Task,
    /// No. of attempt, 1-based.
    pub attempt: u16,
}

impl<Task> Attempt<Task> {
    /// Construct a new first attempt.
    pub fn new(task: Task) -> Self {
        Attempt { task, attempt: 1 }
    }

    pub fn into_inner(self) -> Task {
        self.task
    }
}

/// High level interface for applications to plug into the scheduler.
#[async_trait]
pub trait TaskCoordinator: Send + Sync {
    /// Actual task type sent to executors.
    type Task: Send + Sync;

    /// Result returned from executors.
    type Result: Send + Sync;

    /// Encode a Task for sending it to executors.
    fn encode_task(&self, task: &Self::Task) -> Vec<u8>;

    /// Decode a Result from a received buffer sent by executors.
    fn decode_result(&self, input: Vec<u8>) -> Option<Self::Result>;

    /// Offer to schedule a batch of tasks. The result may be
    /// empty.
    async fn resource_offer(&self) -> Vec<Attempt<Self::Task>>;

    /// Called when a task successfully ended.
    async fn task_successful(&self, attempt: Attempt<Self::Task>, result: Self::Result);

    /// Called when a task completed with a failure.
    async fn task_failed(&self, attempt: Attempt<Self::Task>, reason: FailedReason);
}

/// A convenience implementation for types that implement Deref.
#[async_trait]
impl<T> TaskCoordinator for T
where
    T: Deref + Send + Sync,
    T::Target: TaskCoordinator,
{
    type Task = <T::Target as TaskCoordinator>::Task;

    type Result = <T::Target as TaskCoordinator>::Result;

    #[inline]
    fn encode_task(&self, task: &Self::Task) -> Vec<u8> {
        TaskCoordinator::encode_task(&**self, task)
    }

    #[inline]
    fn decode_result(&self, input: Vec<u8>) -> Option<Self::Result> {
        TaskCoordinator::decode_result(&**self, input)
    }

    #[inline]
    async fn resource_offer(&self) -> Vec<Attempt<Self::Task>> {
        TaskCoordinator::resource_offer(&**self).await
    }

    #[inline]
    async fn task_successful(&self, attempt: Attempt<Self::Task>, result: Self::Result) {
        TaskCoordinator::task_successful(&**self, attempt, result).await
    }

    #[inline]
    async fn task_failed(&self, attempt: Attempt<Self::Task>, reason: FailedReason) {
        TaskCoordinator::task_failed(&**self, attempt, reason).await
    }
}

pub mod tests {

    use super::*;

    use crate::task_runner::ForkScheduler;
    use crate::task_runner::TaskRunner;

    use async_std::sync::Mutex;
    use async_std::task;
    use avro_rs::Reader;
    use avro_rs::Writer;
    use serde::Deserialize;
    use serde::Serialize;
    use slog;
    use std::collections::VecDeque;
    use std::error::Error;
    use std::iter;
    use std::process::Command;
    use std::process::Stdio;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::process::Child;
    use tokio::sync::mpsc;

    /// Represents the MiddleMile stages
    #[derive(Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
    pub enum Stage {
        /// A tasks that Spawns a new MiddleMile scheduler. Only emitted by
        /// InitialScheduler.
        SpawnScheduler(usize, usize),
        ItemDiscovery,
        Elab,
        Opt,
        Sim,
        PolicyPublish,
    }

    impl Stage {
        /// Average output size in bytes
        fn average_output_size(self) -> usize {
            1024 * match self {
                Self::SpawnScheduler(_, _) => 1,
                Self::ItemDiscovery => 1,
                Self::Elab => 410,
                Self::Opt => 593,
                Self::Sim => 1253,
                Self::PolicyPublish => 1,
            }
        }

        fn average_processing_time(self) -> Duration {
            match self {
                Self::SpawnScheduler(_, _) => Duration::from_millis(1),
                Self::ItemDiscovery => Duration::from_millis(20),
                Self::Elab => Duration::from_millis(4000),
                Self::Opt => Duration::from_millis(1400),
                Self::Sim => Duration::from_millis(2100),
                Self::PolicyPublish => Duration::from_millis(78),
            }
        }

        fn next_stages(self) -> Vec<Stage> {
            match self {
                Self::SpawnScheduler(_, _) => vec![],
                Self::ItemDiscovery => vec![Stage::Elab],
                Self::Elab => vec![Stage::Opt],
                Self::Opt => vec![Stage::PolicyPublish, Stage::Sim],
                Self::Sim => vec![],
                Self::PolicyPublish => vec![],
            }
        }

        fn stage_command(
            self,
            run_id: &str,
            config_path: &str,
            input_path: &str,
            stage_name: &str,
        ) -> Child {
            tokio::process::Command::new("new-pipeline")
                .arg("--run-id")
                .arg(run_id)
                .arg("run-stage-io")
                .arg(stage_name)
                .arg("--run")
                .arg(config_path)
                .arg("--input")
                .arg(input_path)
                .arg("--run-hash")
                .arg("12345")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("Could not spawn new-pipeline")
        }

        fn get_child_handle(self, run_id: &str, config_path: &str, input_path: &str) -> Child {
            match self {
                Self::Elab => self.stage_command(run_id, config_path, input_path, "mm-elab"),
                Self::Opt => self.stage_command(run_id, config_path, input_path, "mm-opt"),
                Self::Sim => {
                    self.stage_command(run_id, config_path, input_path, "mm-sim-unconstrained")
                }
                _ => unimplemented!(),
            }
        }
    }

    /// Scheduler 1 and Scheduler 2
    ///    1                 2

    /// InitialCoordinator forks two TestCoordinators where each
    /// schedules half of the MiddleMile tasks.
    #[derive(Clone)]
    pub struct InitialCoordinator {
        logger: slog::Logger,
        to_schedule: Arc<Mutex<Vec<Task>>>,
    }

    impl InitialCoordinator {
        pub fn new(logger: slog::Logger) -> Self {
            Self::with_num_items(logger, 2)
        }

        pub fn with_num_items(logger: slog::Logger, schedulers: usize) -> Self {
            Self {
                logger,
                to_schedule: Arc::new(Mutex::new(
                    (1..schedulers + 1) // Rust ranges are exclusive
                        .map(|ith_scheduler| {
                            Task::new(
                                Stage::SpawnScheduler(schedulers, ith_scheduler),
                                Stage::SpawnScheduler(schedulers, ith_scheduler),
                                ith_scheduler,
                            )
                        })
                        .collect(),
                )),
            }
        }
    }

    pub struct MiddleMileCoordinator {
        logger: slog::Logger,
        to_schedule: Arc<Mutex<VecDeque<Task>>>,
    }

    impl MiddleMileCoordinator {
        pub fn new(
            logger: slog::Logger,
            run_id: &str,
            config_path: &str,
            input_path: &str,
            total_coordinators: usize,
            nth_coordinator: usize,
        ) -> Self {
            let items_proc = Command::new("new-pipeline")
                .arg("--run-id")
                .arg(run_id)
                .arg("run-stage-io")
                .arg("mm-items")
                .arg("--run")
                .arg(config_path)
                .arg("--input")
                .arg(input_path)
                .arg("--run-hash")
                .arg("12345")
                .stdout(Stdio::piped())
                .spawn()
                .expect("Could not spawn new-pipeline process");

            let items_stdout = items_proc.stdout.expect("Stdout not overriden");
            let items_reader = Reader::new(items_stdout).expect("Could decode output of mm-items");

            let writer_schema = items_reader.writer_schema().clone();

            // Ugh, unbounded channel is bad but we can't block in a thread
            let (send_item, mut receive_item) = mpsc::unbounded_channel();
            let logger1 = logger.clone();

            std::thread::spawn(move || {
                for (i, item) in items_reader.enumerate() {
                    if (i + 1) % total_coordinators != nth_coordinator - 1 {
                        continue;
                    }

                    match item {
                        Ok(avro_rs::types::Value::Record(fields)) => {
                            let (_name, value) = fields.into_iter().next().expect("No fields");
                            match value {
                                avro_rs::types::Value::Bytes(bytes) => {
                                    let output = Vec::with_capacity(2 * bytes.len());
                                    let mut writer = Writer::new(&writer_schema, output);

                                    writer
                                        .append(avro_rs::types::Value::Record(vec![(
                                            "bytes".to_string(),
                                            avro_rs::types::Value::Bytes(bytes),
                                        )]))
                                        .expect("Writing avro record failed");

                                    writer.flush().expect("Flushing writer failed");

                                    send_item
                                        .send(writer.into_inner())
                                        .expect("Receiver must live at least as long as sender")
                                }
                                _ => {
                                    slog::error!(logger1, "Not valid bytes");
                                    panic!("Not valid bytes")
                                }
                            }
                        }
                        _ => {
                            slog::error!(logger1, "Could not decode item");
                            panic!("Could not decode item")
                        }
                    }
                }
            });

            let to_schedule = Arc::new(Mutex::new(VecDeque::new()));
            let to_schedule1 = to_schedule.clone();

            tokio::spawn(async move {
                let mut seq = 0;

                while let Some(item) = receive_item.recv().await {
                    let task = Task {
                        stage: Stage::Elab,
                        seq_no: seq,
                        input: item,
                    };

                    to_schedule1.lock().await.push_back(task);
                    seq += 1;
                }
            });

            Self {
                logger,
                to_schedule,
            }
        }
    }

    #[async_trait]
    impl TaskCoordinator for MiddleMileCoordinator {
        type Task = Task;

        type Result = Result;

        async fn resource_offer(&self) -> Vec<Attempt<Self::Task>> {
            let batch_size = 10;
            let mut tasks_to_schedule = Vec::with_capacity(batch_size);

            // Schedule successsor tasks first, then schedule new item discovery
            // tasks
            {
                let mut to_schedule = self.to_schedule.lock().await;
                for _i in 1..batch_size {
                    if to_schedule.is_empty() {
                        break;
                    }

                    let task = to_schedule.pop_front().expect("Should not be empty");
                    tasks_to_schedule.push(Attempt::new(task));
                }
            }

            tasks_to_schedule
        }

        fn encode_task(&self, task: &Self::Task) -> Vec<u8> {
            serde_cbor::to_vec(task).expect("Could not serialize")
        }

        fn decode_result(&self, input: Vec<u8>) -> Option<Self::Result> {
            serde_cbor::from_slice(&input).ok()
        }

        async fn task_successful(&self, attempt: Attempt<Self::Task>, result: Self::Result) {
            let task = &attempt.task;
            slog::info!(self.logger, "Task completed successfully";
                        "seq" => task.seq_no,
                        "stage" => format!("{:?}", task.stage)
            );

            let mut to_schedule = self.to_schedule.lock().await;
            for task in result.successor_tasks() {
                to_schedule.push_front(task);
            }
        }

        async fn task_failed(&self, attempt: Attempt<Self::Task>, _reason: FailedReason) {
            let task = &attempt.task;
            slog::warn!(self.logger, "Task failed";
                        "seq" => task.seq_no,
                        "stage" => format!("{:?}", task.stage)
            );
        }
    }

    #[derive(Debug)]
    struct StageError;

    impl std::fmt::Display for StageError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "StageError")
        }
    }

    impl std::error::Error for StageError {}

    pub struct MiddleMileRunner {
        logger: slog::Logger,
        run_id: String,
        config_path: String,
        input_path: String,
    }

    impl MiddleMileRunner {
        pub fn new(
            logger: slog::Logger,
            run_id: &str,
            config_path: &str,
            input_path: &str,
        ) -> Self {
            Self {
                logger,
                run_id: run_id.to_string(),
                config_path: config_path.to_string(),
                input_path: input_path.to_string(),
            }
        }
    }

    #[async_trait]
    impl TaskRunner for MiddleMileRunner {
        type Task = Task;

        type Result = Result;

        fn decode_task(&self, input: Vec<u8>) -> Option<Self::Task> {
            match serde_cbor::from_slice(&input) {
                Ok(x) => Some(x),
                Err(_) => None,
            }
        }

        fn encode_result(&self, result: Self::Result) -> Vec<u8> {
            serde_cbor::to_vec(&result).expect("Could not serialize")
        }

        async fn run_task<Fork>(
            &self,
            fork: &Fork,
            task: Self::Task,
        ) -> std::result::Result<Self::Result, Box<dyn Error>>
        where
            Fork: ForkScheduler,
        {
            slog::info!(self.logger, "Running task";
                        "seq" => task.seq_no,
                        "stage" => format!("{:?}", task.stage)
            );

            if let Stage::SpawnScheduler(schedulers, ith_coordinator) = task.stage {
                fork
                    .fork(MiddleMileCoordinator::new(self.logger.clone(), &self.run_id, &self.config_path, &self.input_path, schedulers, ith_coordinator))
                    .await
                    .map_err(|error| {
                        slog::error!(self.logger, "Forked scheduler failed"; "error" => error.to_string());
                        error
                    })?;

                Ok(Result {
                    from_stage: task.stage,
                    seq_no: task.seq_no,
                    output: vec![],
                })
            } else if task.stage == Stage::Opt
                || task.stage == Stage::Sim
                || task.stage == Stage::Elab
            {
                let mut child =
                    task.stage
                        .get_child_handle(&self.run_id, &self.config_path, &self.input_path);

                child
                    .stdin
                    .as_mut()
                    .expect("No STDIN")
                    .write_all(&task.input)
                    .await?;

                let output = child.wait_with_output().await?;

                if output.status.success() {
                    Ok(Result {
                        from_stage: task.stage,
                        seq_no: task.seq_no,
                        output: output.stdout,
                    })
                } else {
                    Err(Box::new(StageError))
                }
            } else {
                Ok(task.run().await)
            }
        }
    }

    /// TestCoordinator runs the MiddleMile pipeline for a set of items.
    /// It also knows how to actually execute the tasks.
    pub struct TestCoordinator {
        logger: slog::Logger,
        limit: usize,
        state: AtomicUsize,
        spawned_tasks: AtomicUsize,
        successful_tasks: AtomicUsize,
        failed_tasks: AtomicUsize,
        to_schedule: Mutex<VecDeque<Task>>,
    }

    impl TestCoordinator {
        pub fn new(logger: slog::Logger) -> Self {
            Self::with_limit(logger, 1)
        }

        pub fn with_limit(logger: slog::Logger, limit: usize) -> Self {
            Self {
                logger,
                limit,
                state: AtomicUsize::new(0),
                spawned_tasks: AtomicUsize::new(0),
                successful_tasks: AtomicUsize::new(0),
                failed_tasks: AtomicUsize::new(0),
                to_schedule: Mutex::new(VecDeque::new()),
            }
        }
    }

    #[async_trait]
    impl TaskRunner for TestCoordinator {
        type Task = Task;

        type Result = Result;

        fn decode_task(&self, input: Vec<u8>) -> Option<Self::Task> {
            match serde_cbor::from_slice(&input) {
                Ok(x) => Some(x),
                Err(_) => None,
            }
        }

        fn encode_result(&self, result: Self::Result) -> Vec<u8> {
            serde_cbor::to_vec(&result).expect("Could not serialize")
        }

        async fn run_task<Fork>(
            &self,
            fork_scheduler: &Fork,
            task: Self::Task,
        ) -> std::result::Result<Self::Result, Box<dyn Error>>
        where
            Fork: ForkScheduler,
        {
            slog::info!(self.logger, "Running task";
                        "seq" => task.seq_no,
                        "stage" => format!("{:?}", task.stage)
            );

            if let Stage::SpawnScheduler(_, _) = task.stage {
                fork_scheduler
                    .fork(TestCoordinator::with_limit(self.logger.clone(), task.seq_no))
                    .await
                    .map_err(|error| {
                        slog::error!(self.logger, "Forked scheduler failed"; "error" => error.to_string());
                        error
                    })?;
            }

            Ok(task.run().await)
        }
    }

    #[async_trait]
    impl TaskCoordinator for InitialCoordinator {
        type Task = Task;

        type Result = Result;

        async fn resource_offer(&self) -> Vec<Attempt<Self::Task>> {
            let task = self.to_schedule.lock().await.pop();
            match task {
                Some(t) => vec![Attempt::new(t)],
                None => vec![],
            }
        }

        fn encode_task(&self, task: &Self::Task) -> Vec<u8> {
            serde_cbor::to_vec(task).expect("Could not serialize")
        }

        fn decode_result(&self, input: Vec<u8>) -> Option<Self::Result> {
            serde_cbor::from_slice(&input).ok()
        }

        async fn task_successful(&self, _attempt: Attempt<Self::Task>, _result: Self::Result) {
            slog::info!(self.logger, "Yay, we are super happy");
        }

        async fn task_failed(&self, _attempt: Attempt<Self::Task>, _reason: FailedReason) {
            slog::info!(self.logger, "Meeeeeh");
        }
    }

    #[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
    pub struct Task {
        stage: Stage,
        seq_no: usize,
        input: Vec<u8>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Result {
        from_stage: Stage,
        seq_no: usize,
        output: Vec<u8>,
    }

    impl Result {
        fn successor_tasks(self) -> Vec<Task> {
            self.from_stage
                .next_stages()
                .into_iter()
                .map(|stage| Task {
                    stage,
                    seq_no: self.seq_no + 1,
                    input: self.output.clone(),
                })
                .collect()
        }
    }

    impl Task {
        fn new(prev_stage: Stage, stage: Stage, seq_no: usize) -> Task {
            Task {
                stage,
                seq_no,
                input: iter::repeat(0 as u8)
                    .take(prev_stage.average_output_size())
                    .collect(),
            }
        }

        async fn run(&self) -> Result {
            task::sleep(self.stage.average_processing_time()).await;
            Result {
                from_stage: self.stage,
                seq_no: self.seq_no,
                output: iter::repeat(0 as u8)
                    .take(self.stage.average_output_size())
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl TaskCoordinator for TestCoordinator {
        type Task = Task;

        type Result = Result;

        async fn resource_offer(&self) -> Vec<Attempt<Self::Task>> {
            slog::debug!(self.logger, "Resource offer");

            let batch_size = 10;
            let mut tasks_to_schedule = Vec::with_capacity(batch_size);

            // Schedule successsor tasks first, then schedule new item discovery
            // tasks
            {
                let mut to_schedule = self.to_schedule.lock().await;

                // if !to_schedule.is_empty() && to_schedule.len() < batch_size {
                //     return vec![]
                // }

                for _i in 1..batch_size {
                    if to_schedule.is_empty() {
                        break;
                    }

                    let task = to_schedule.pop_front().expect("Should not be empty");
                    tasks_to_schedule.push(Attempt::new(task));
                }
            }

            let current = self.state.load(Ordering::Relaxed);
            if current >= self.limit {
                // Ugh, early exits, very imperative
                return tasks_to_schedule;
            }

            let remaining_slots = batch_size - tasks_to_schedule.len();

            let start = self.state.fetch_add(remaining_slots, Ordering::Relaxed);

            let item_disc_tasks: Vec<Attempt<Self::Task>> = (start..start + remaining_slots)
                .into_iter()
                .map(|seq| Attempt::new(Task::new(Stage::ItemDiscovery, Stage::Elab, seq)))
                .collect();

            tasks_to_schedule.extend(item_disc_tasks);

            let tasks = tasks_to_schedule;

            self.spawned_tasks.fetch_add(batch_size, Ordering::Relaxed);

            slog::debug!(self.logger, "Resource offer complete");

            tasks
        }

        fn encode_task(&self, task: &Self::Task) -> Vec<u8> {
            serde_cbor::to_vec(task).expect("Could not serialize")
        }

        fn decode_result(&self, input: Vec<u8>) -> Option<Self::Result> {
            serde_cbor::from_slice(&input).ok()
        }

        async fn task_successful(&self, attempt: Attempt<Task>, result: Self::Result) {
            let task = &attempt.task;
            self.successful_tasks.fetch_add(1, Ordering::Relaxed);

            slog::info!(self.logger, "Task completed successfully";
                        "seq" => task.seq_no,
                        "stage" => format!("{:?}", task.stage)
            );

            self.to_schedule
                .lock()
                .await
                .extend(result.successor_tasks());
        }

        async fn task_failed(&self, _attempt: Attempt<Self::Task>, _reason: FailedReason) {
            self.failed_tasks.fetch_add(1, Ordering::Relaxed);
        }
    }
}
