use std::collections::HashMap;

use common_error::DaftResult;

use super::task::{SwordfishTask, SwordfishTaskResultHandle};

#[allow(dead_code)]
pub(crate) trait WorkerManager: Send + Sync {
    fn submit_task_to_worker(
        &self,
        task: SwordfishTask,
        worker_id: String,
    ) -> Box<dyn SwordfishTaskResultHandle>;
    // (worker id, num_slots)
    fn get_worker_slots(&self) -> HashMap<String, usize>;
    #[allow(dead_code)]
    fn try_autoscale(&self, num_workers: usize) -> DaftResult<()>;
    fn shutdown(&self) -> DaftResult<()>;
}

pub(crate) trait WorkerManagerFactory: Send + Sync {
    fn create_worker_manager(&self) -> DaftResult<Box<dyn WorkerManager>>;
}
