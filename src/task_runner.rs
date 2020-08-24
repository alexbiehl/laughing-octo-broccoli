use crate::scheduler::TaskCoordinator;

use async_trait::async_trait;
use std::error::Error;
use std::ops::Deref;

/// A trait to run a new scheduler. A new scheduler
/// will be launched and run till completion.
#[async_trait]
pub trait ForkScheduler: Send + Sync {
    async fn fork<Coordinator>(&self, task_coordinator: Coordinator) -> Result<(), Box<dyn Error>>
    where
        Coordinator: 'static + TaskCoordinator;
}

#[async_trait]
pub trait TaskRunner: Send + Sync {
    type Task: Send + Sync;

    type Result: Send + Sync;

    fn decode_task(&self, input: Vec<u8>) -> Option<Self::Task>;

    fn encode_result(&self, result: Self::Result) -> Vec<u8>;

    async fn run_task<Fork>(
        &self,
        fork: &Fork,
        task: Self::Task,
    ) -> Result<Self::Result, Box<dyn Error>>
    where
        Fork: ForkScheduler;
}

#[async_trait]
impl<T> TaskRunner for T
where
    T: Deref + Send + Sync,
    T::Target: TaskRunner,
{
    type Task = <T::Target as TaskRunner>::Task;

    type Result = <T::Target as TaskRunner>::Result;

    #[inline]
    fn decode_task(&self, input: Vec<u8>) -> Option<Self::Task> {
        TaskRunner::decode_task(&**self, input)
    }

    #[inline]
    fn encode_result(&self, result: Self::Result) -> Vec<u8> {
        TaskRunner::encode_result(&**self, result)
    }

    #[inline]
    async fn run_task<Fork>(
        &self,
        fork: &Fork,
        task: Self::Task,
    ) -> Result<Self::Result, Box<dyn Error>>
    where
        Fork: ForkScheduler,
    {
        TaskRunner::run_task(&**self, fork, task).await
    }
}
