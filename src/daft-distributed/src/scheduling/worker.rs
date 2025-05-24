use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;

use super::task::{Task, TaskDetails, TaskId, TaskResultHandle};

pub(crate) type WorkerId = Arc<str>;

#[allow(dead_code)]
pub(crate) trait Worker: Send + Sync + 'static {
    type Task: Task;
    type TaskResultHandle: TaskResultHandle;

    fn id(&self) -> &WorkerId;
    fn active_task_details(&self) -> HashMap<TaskId, TaskDetails>;
    fn total_num_cpus(&self) -> usize;
    fn active_num_cpus(&self) -> usize;
    fn available_num_cpus(&self) -> usize;
}

#[allow(dead_code)]
pub(crate) trait WorkerManager: Send + Sync {
    type Worker: Worker;

    fn submit_tasks_to_workers(
        &self,
        total_tasks: usize,
        tasks_per_worker: HashMap<WorkerId, Vec<<<Self as WorkerManager>::Worker as Worker>::Task>>,
    ) -> DaftResult<Vec<<<Self as WorkerManager>::Worker as Worker>::TaskResultHandle>>;
    fn mark_task_finished(&self, task_id: TaskId, worker_id: WorkerId);
    fn workers(&self) -> &HashMap<WorkerId, Self::Worker>;
    fn total_available_cpus(&self) -> usize;
    #[allow(dead_code)]
    fn try_autoscale(&self, _num_workers: usize) -> DaftResult<()> {
        Ok(())
    }
    fn shutdown(&self) -> DaftResult<()>;
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::{atomic::AtomicBool, Mutex};

    use super::*;
    use crate::scheduling::task::tests::{MockTask, MockTaskResultHandle};
    /// A mock implementation of the WorkerManager trait for testing
    #[derive(Clone)]
    pub struct MockWorkerManager {
        workers: HashMap<WorkerId, MockWorker>,
    }

    impl MockWorkerManager {
        pub fn new() -> Self {
            Self {
                workers: HashMap::new(),
            }
        }

        pub fn add_worker(&mut self, worker_id: WorkerId, num_cpus: usize) -> DaftResult<()> {
            self.workers
                .insert(worker_id.clone(), MockWorker::new(worker_id, num_cpus));
            Ok(())
        }
    }

    impl WorkerManager for MockWorkerManager {
        type Worker = MockWorker;

        fn submit_tasks_to_workers(
            &self,
            total_tasks: usize,
            tasks_per_worker: HashMap<
                WorkerId,
                Vec<<<Self as WorkerManager>::Worker as Worker>::Task>,
            >,
        ) -> DaftResult<Vec<<<Self as WorkerManager>::Worker as Worker>::TaskResultHandle>>
        {
            let mut result = Vec::new();

            for (worker_id, tasks) in tasks_per_worker {
                for task in tasks {
                    // Update the worker's active task count
                    if let Some(worker) = self.workers.get(&worker_id) {
                        worker.add_active_task(&task);
                    }

                    result.push(MockTaskResultHandle::new(
                        self.clone(),
                        worker_id.clone(),
                        task,
                    ));
                }
            }

            Ok(result)
        }

        fn mark_task_finished(&self, task_id: TaskId, worker_id: WorkerId) {
            if let Some(worker) = self.workers.get(&worker_id) {
                worker.mark_task_finished(&task_id);
            }
        }

        fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
            &self.workers
        }

        fn total_available_cpus(&self) -> usize {
            self.workers
                .values()
                .map(|w| w.total_num_cpus() - w.active_num_cpus())
                .sum()
        }

        fn try_autoscale(&self, _num_workers: usize) -> DaftResult<()> {
            // No-op for mock implementation
            Ok(())
        }

        fn shutdown(&self) -> DaftResult<()> {
            self.workers.values().for_each(|w| w.shutdown());
            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct MockWorker {
        worker_id: WorkerId,
        total_num_cpus: usize,
        active_task_details: Arc<Mutex<HashMap<TaskId, TaskDetails>>>,
        is_shutdown: Arc<AtomicBool>,
    }

    impl MockWorker {
        pub fn new(worker_id: WorkerId, total_num_cpus: usize) -> Self {
            Self {
                worker_id,
                total_num_cpus,
                active_task_details: Arc::new(Mutex::new(HashMap::new())),
                is_shutdown: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn mark_task_finished(&self, task_id: &TaskId) {
            self.active_task_details.lock().unwrap().remove(task_id);
        }

        pub fn add_active_task(&self, task: &impl Task) {
            self.active_task_details
                .lock()
                .unwrap()
                .insert(task.task_id().clone(), TaskDetails::from(task));
        }

        pub fn shutdown(&self) {
            self.is_shutdown
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Worker for MockWorker {
        type Task = MockTask;
        type TaskResultHandle = MockTaskResultHandle;

        fn id(&self) -> &WorkerId {
            &self.worker_id
        }

        fn total_num_cpus(&self) -> usize {
            self.total_num_cpus
        }

        fn active_num_cpus(&self) -> usize {
            let active_task_details = self.active_task_details.lock().unwrap();

            active_task_details
                .values()
                .map(|details| details.num_cpus())
                .sum()
        }

        fn available_num_cpus(&self) -> usize {
            self.total_num_cpus() - self.active_num_cpus()
        }

        fn active_task_details(&self) -> HashMap<TaskId, TaskDetails> {
            self.active_task_details
                .lock()
                .expect("Active task ids should be present")
                .clone()
        }
    }
}
