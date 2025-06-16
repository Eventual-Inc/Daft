use std::{collections::HashMap, fmt::Debug, sync::Arc};

use common_error::DaftResult;

use super::{
    scheduler::SchedulableTask,
    task::{Task, TaskDetails, TaskID, TaskResultHandle, TaskResultHandleAwaiter},
};

pub(crate) type WorkerId = Arc<str>;

#[allow(dead_code)]
pub(crate) trait Worker: Send + Sync + Debug + 'static {
    type Task: Task;
    type TaskResultHandle: TaskResultHandle;

    fn id(&self) -> &WorkerId;
    fn active_task_details(&self) -> HashMap<TaskID, TaskDetails>;
    fn total_num_cpus(&self) -> f64;
    fn total_num_gpus(&self) -> f64;
    fn active_num_cpus(&self) -> f64;
    fn active_num_gpus(&self) -> f64;
    fn available_num_cpus(&self) -> f64 {
        self.total_num_cpus() - self.active_num_cpus()
    }
    fn available_num_gpus(&self) -> f64 {
        self.total_num_gpus() - self.active_num_gpus()
    }
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
    fn mark_task_finished(&self, task_id: &TaskID, worker_id: &WorkerId);
    fn workers(&self) -> &HashMap<WorkerId, Self::Worker>;
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
    use crate::scheduling::tests::{MockTask, MockTaskResultHandle};

    /// A mock implementation of the WorkerManager trait for testing
    #[derive(Clone)]
    pub struct MockWorkerManager {
        workers: HashMap<WorkerId, MockWorker>,
    }

    impl MockWorkerManager {
        pub fn new(workers: HashMap<WorkerId, MockWorker>) -> Self {
            Self { workers }
        }
    }

    impl WorkerManager for MockWorkerManager {
        type Worker = MockWorker;

        fn submit_tasks_to_workers(
            &self,
            tasks_per_worker: HashMap<
                WorkerId,
                Vec<SchedulableTask<<<Self as WorkerManager>::Worker as Worker>::Task>>,
            >,
        ) -> DaftResult<
            Vec<
                TaskResultHandleAwaiter<
                    <<Self as WorkerManager>::Worker as Worker>::TaskResultHandle,
                >,
            >,
        > {
            let mut result = Vec::new();

            for (worker_id, tasks) in tasks_per_worker {
                for task in tasks {
                    let (task, result_tx, cancel_token) = task.into_inner();
                    // Update the worker's active task count
                    if let Some(worker) = self.workers.get(&worker_id) {
                        worker.add_active_task(&task);
                    }

                    result.push(TaskResultHandleAwaiter::new(
                        Arc::from(task.task_id().to_string()),
                        worker_id.clone(),
                        MockTaskResultHandle::new(task),
                        result_tx,
                        cancel_token,
                    ));
                }
            }

            Ok(result)
        }

        fn mark_task_finished(&self, task_id: &TaskID, worker_id: &WorkerId) {
            if let Some(worker) = self.workers.get(worker_id) {
                worker.mark_task_finished(task_id);
            }
        }

        fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
            &self.workers
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

    #[derive(Clone, Debug)]
    pub struct MockWorker {
        worker_id: WorkerId,
        total_num_cpus: f64,
        total_num_gpus: f64,
        active_task_details: Arc<Mutex<HashMap<TaskID, TaskDetails>>>,
        is_shutdown: Arc<AtomicBool>,
    }

    impl MockWorker {
        pub fn new(worker_id: WorkerId, total_num_cpus: f64, total_num_gpus: f64) -> Self {
            Self {
                worker_id,
                total_num_cpus,
                total_num_gpus,
                active_task_details: Arc::new(Mutex::new(HashMap::new())),
                is_shutdown: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn mark_task_finished(&self, task_id: &TaskID) {
            self.active_task_details.lock().unwrap().remove(task_id);
        }

        pub fn add_active_task(&self, task: &impl Task) {
            self.active_task_details.lock().unwrap().insert(
                Arc::from(task.task_id().to_string()),
                TaskDetails::from(task),
            );
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

        fn total_num_cpus(&self) -> f64 {
            self.total_num_cpus
        }

        fn total_num_gpus(&self) -> f64 {
            self.total_num_gpus
        }

        fn active_num_cpus(&self) -> f64 {
            self.active_task_details
                .lock()
                .expect("Active task ids should be present")
                .values()
                .map(|details| details.num_cpus())
                .sum()
        }

        fn active_num_gpus(&self) -> f64 {
            self.active_task_details
                .lock()
                .expect("Active task ids should be present")
                .values()
                .map(|details| details.num_gpus())
                .sum()
        }

        fn active_task_details(&self) -> HashMap<TaskID, TaskDetails> {
            self.active_task_details
                .lock()
                .expect("Active task ids should be present")
                .clone()
        }
    }
}
