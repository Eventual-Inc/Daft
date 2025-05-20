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
                println!("Submitting tasks to worker: {:?}", worker_id);
                for task in tasks {
                    println!("Submitting task: {:?}", task.task_id());
                    let (task, result_tx, cancel_token) = task.into_inner();
                    // Update the worker's active task count
                    if let Some(worker) = self.workers.get(&worker_id) {
                        worker.add_active_task(task.task_id().clone());
                    }

                    result.push(TaskResultHandleAwaiter::new(
                        task.task_id().clone(),
                        worker_id.clone(),
                        MockTaskResultHandle::new(self.clone(), worker_id.clone(), task),
                        result_tx,
                        cancel_token,
                    ));
                }
            }

            Ok(result)
        }

        fn mark_task_finished(&self, task_id: &TaskId, worker_id: &WorkerId) {
            if let Some(worker) = self.workers.get(worker_id) {
                worker.mark_task_finished(task_id);
            }
        }

        fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
            &self.workers
        }

        fn total_available_cpus(&self) -> usize {
            self.workers.values().map(|w| w.num_cpus).sum()
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
        num_cpus: usize,
        num_active_tasks: usize,
        active_task_ids: Arc<Mutex<HashSet<TaskId>>>,
        is_shutdown: Arc<AtomicBool>,
    }

    impl MockWorker {
        pub fn new(worker_id: WorkerId, num_cpus: usize) -> Self {
            Self {
                worker_id,
                num_cpus,
                num_active_tasks: 0,
                active_task_ids: Arc::new(Mutex::new(HashSet::new())),
                is_shutdown: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn mark_task_finished(&self, task_id: &TaskId) {
            self.active_task_ids.lock().unwrap().remove(task_id);
        }

        pub fn add_active_task(&self, task_id: TaskId) {
            self.active_task_ids.lock().unwrap().insert(task_id);
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

        fn num_cpus(&self) -> usize {
            self.num_cpus
        }

        fn active_task_ids(&self) -> HashSet<TaskId> {
            self.active_task_ids.lock().unwrap().clone()
        }
    }

    // Helper function to create workers with given configurations
    pub fn setup_workers(configs: &[(WorkerId, usize)]) -> HashMap<WorkerId, MockWorker> {
        configs
            .iter()
            .map(|(id, num_slots)| {
                let worker = MockWorker::new(id.clone(), *num_slots);
                (id.clone(), worker)
            })
            .collect::<HashMap<_, _>>()
    }
}
