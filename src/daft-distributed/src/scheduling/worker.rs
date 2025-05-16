use std::collections::{HashMap, HashSet};

use common_error::DaftResult;

use super::task::{SwordfishTaskResultHandle, Task, TaskId};

pub(crate) type WorkerId = String;

pub(crate) trait Worker: Send + Sync + 'static {
    fn id(&self) -> &WorkerId;
    fn num_cpus(&self) -> usize;
    fn active_task_ids(&self) -> HashSet<TaskId>;
}

#[allow(dead_code)]
pub(crate) trait WorkerManager: Send + Sync {
    type Worker: Worker;

    fn submit_task_to_worker(
        &self,
        task: Box<dyn Task>,
        worker_id: WorkerId,
    ) -> Box<dyn SwordfishTaskResultHandle>;
    fn mark_task_finished(&self, task_id: TaskId, worker_id: WorkerId);
    fn workers(&self) -> &HashMap<WorkerId, Self::Worker>;
    fn total_available_cpus(&self) -> usize;
    #[allow(dead_code)]
    fn try_autoscale(&self, _num_workers: usize) -> DaftResult<()> {
        Ok(())
    }
    fn shutdown(&self) -> DaftResult<()>;
}
