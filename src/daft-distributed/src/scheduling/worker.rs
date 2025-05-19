use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;

use super::{
    scheduler::SchedulableTask,
    task::{Task, TaskId, TaskResultHandle, TaskResultHandleAwaiter},
};

pub(crate) type WorkerId = Arc<str>;

#[allow(dead_code)]
pub(crate) trait Worker: Send + Sync + 'static {
    type Task: Task;
    type TaskResultHandle: TaskResultHandle;

    fn id(&self) -> &WorkerId;
    fn num_cpus(&self) -> usize;
    fn active_task_ids(&self) -> HashSet<TaskId>;
}

#[allow(dead_code)]
pub(crate) trait WorkerManager: Send + Sync {
    type Worker: Worker;

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<
            WorkerId,
            Vec<SchedulableTask<<<Self as WorkerManager>::Worker as Worker>::Task>>,
        >,
    ) -> DaftResult<
        Vec<TaskResultHandleAwaiter<<<Self as WorkerManager>::Worker as Worker>::TaskResultHandle>>,
    >;
    fn mark_task_finished(&self, task_id: &TaskId, worker_id: &WorkerId);
    fn workers(&self) -> &HashMap<WorkerId, Self::Worker>;
    fn total_available_cpus(&self) -> usize;
    #[allow(dead_code)]
    fn try_autoscale(&self, _num_workers: usize) -> DaftResult<()> {
        Ok(())
    }
    fn shutdown(&self) -> DaftResult<()>;
}
