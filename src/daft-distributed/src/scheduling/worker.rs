use super::task::{SwordfishTask, SwordfishTaskResultHandle};

pub(crate) trait WorkerManager: Send + Sync {
    fn submit_task_to_worker(
        &self,
        task: SwordfishTask,
        worker_id: String,
    ) -> Box<dyn SwordfishTaskResultHandle>;
    // (worker id, num_cpus, memory)
    fn get_worker_resources(&self) -> Vec<(String, usize, usize)>;
    #[allow(dead_code)]
    fn try_autoscale(&self, num_workers: usize);
    fn shutdown(&self);
}
