use crate::task::{Task, TaskHandle};

pub(crate) trait WorkerManager: Send + Sync {
    fn submit_task_to_worker(&self, task: Task, worker_id: String) -> TaskHandle;
    // (worker id, num_cpus, memory)
    fn get_worker_resources(&self) -> Vec<(String, usize, usize)>;
    fn try_autoscale(&self, num_workers: usize) -> ();
}
