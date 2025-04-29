use std::sync::Arc;

use common_error::DaftResult;

use super::task::{SwordfishTask, SwordfishTaskResultHandle};

pub(crate) type WorkerManagerCreator =
    Arc<dyn Fn() -> DaftResult<Box<dyn WorkerManager>> + Send + Sync>;

#[allow(dead_code)]
pub(crate) trait WorkerManager: Send + Sync {
    fn submit_task_to_worker(
        &self,
        task: SwordfishTask,
        worker_id: String,
    ) -> Box<dyn SwordfishTaskResultHandle>;
    // (worker id, num_cpus, memory)
    fn get_worker_resources(&self) -> Vec<(String, usize, usize)>;
    #[allow(dead_code)]
    fn try_autoscale(&self, num_workers: usize) -> DaftResult<()>;
    fn shutdown(&self) -> DaftResult<()>;
}
