use std::{collections::HashMap, fmt::Debug, sync::Arc};

use common_error::DaftResult;

use super::task::{Task, TaskDetails, TaskResultHandle};
use crate::scheduling::{
    scheduler::WorkerSnapshot,
    task::{TaskContext, TaskResourceRequest},
};

pub(crate) type WorkerId = Arc<str>;

pub(crate) trait Worker: Send + Sync + Debug + 'static {
    type Task: Task;
    type TaskResultHandle: TaskResultHandle;

    fn id(&self) -> &WorkerId;
    fn active_task_details(&self) -> HashMap<TaskContext, TaskDetails>;
    fn total_num_cpus(&self) -> f64;
    fn total_num_gpus(&self) -> f64;
    #[allow(dead_code)]
    fn active_num_cpus(&self) -> f64;
    #[allow(dead_code)]
    fn active_num_gpus(&self) -> f64;
    #[allow(dead_code)]
    fn available_num_cpus(&self) -> f64 {
        self.total_num_cpus() - self.active_num_cpus()
    }
    #[allow(dead_code)]
    fn available_num_gpus(&self) -> f64 {
        self.total_num_gpus() - self.active_num_gpus()
    }
}

pub(crate) trait WorkerManager: Send + Sync {
    type Worker: Worker;

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<<<Self as WorkerManager>::Worker as Worker>::Task>>,
    ) -> DaftResult<Vec<<<Self as WorkerManager>::Worker as Worker>::TaskResultHandle>>;
    fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId);
    fn mark_worker_died(&self, worker_id: WorkerId);
    fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>>;
    fn try_autoscale(&self, resource_requests: Vec<TaskResourceRequest>) -> DaftResult<()>;
    #[allow(dead_code)]
    fn shutdown(&self) -> DaftResult<()>;

    /// Retire idle workers; when `force_all_when_cluster_idle` is true, release all idle workers.
    fn retire_idle_workers(
        &self,
        max_to_retire: usize,
        force_all_when_cluster_idle: bool,
    ) -> DaftResult<usize>;
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::{Mutex, atomic::AtomicBool};

    use super::*;
    use crate::scheduling::tests::{MockTask, MockTaskResultHandle};

    /// A mock implementation of the WorkerManager trait for testing
    #[derive(Clone)]
    pub struct MockWorkerManager {
        workers: Arc<Mutex<HashMap<WorkerId, MockWorker>>>,
        try_autoscale_calls: Arc<Mutex<usize>>,
        last_try_autoscale_bundles_len: Arc<Mutex<Option<usize>>>,
        idle_calls: Arc<Mutex<Vec<(usize, bool, Option<u64>)>>>,
    }

    impl MockWorkerManager {
        pub fn new(workers: HashMap<WorkerId, MockWorker>) -> Self {
            Self {
                workers: Arc::new(Mutex::new(workers)),
                try_autoscale_calls: Arc::new(Mutex::new(0)),
                last_try_autoscale_bundles_len: Arc::new(Mutex::new(None)),
                idle_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Test-only accessor: number of try_autoscale calls
        pub fn try_autoscale_call_count(&self) -> usize {
            *self.try_autoscale_calls.lock().expect("lock")
        }

        /// Test-only accessor: last bundles len passed to try_autoscale
        pub fn last_try_autoscale_bundles_len(&self) -> Option<usize> {
            *self.last_try_autoscale_bundles_len.lock().expect("lock")
        }
        /// Test-only accessor: idle cleanup call log
        pub fn idle_calls_log(&self) -> Vec<(usize, bool, Option<u64>)> {
            self.idle_calls.lock().expect("lock").clone()
        }
    }

    impl WorkerManager for MockWorkerManager {
        type Worker = MockWorker;

        fn retire_idle_workers(
            &self,
            max_to_retire: usize,
            force_all_when_cluster_idle: bool,
        ) -> DaftResult<usize> {
            // Read idle threshold from env for logging; Mock does not track durations
            let idle_secs_threshold: u64 = std::env::var("DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(120);
            // Record test-only logs
            {
                let mut calls = self.idle_calls.lock().expect("lock");
                calls.push((
                    max_to_retire,
                    force_all_when_cluster_idle,
                    Some(idle_secs_threshold),
                ));
            }
            let mut workers = self.workers.lock().expect("Failed to lock workers");
            if max_to_retire == 0 || workers.is_empty() {
                return Ok(0);
            }
            // Determine idle candidates (no active tasks)
            let candidate_ids: Vec<WorkerId> = workers
                .iter()
                .filter_map(|(wid, w)| {
                    let active = w.active_task_details.lock().expect("Failed to lock");
                    if active.is_empty() {
                        Some(wid.clone())
                    } else {
                        None
                    }
                })
                .collect();
            let target = if force_all_when_cluster_idle {
                candidate_ids.len().min(max_to_retire)
            } else {
                max_to_retire.min(candidate_ids.len())
            };
            let mut retired = 0usize;
            for wid in candidate_ids.into_iter().take(target) {
                workers.remove(&wid);
                retired += 1;
            }
            Ok(retired)
        }

        fn submit_tasks_to_workers(
            &self,
            tasks_per_worker: HashMap<WorkerId, Vec<MockTask>>,
        ) -> DaftResult<Vec<MockTaskResultHandle>> {
            let mut result = Vec::new();
            for (worker_id, tasks) in tasks_per_worker {
                for task in tasks {
                    // Update the worker's active task count
                    if let Some(worker) = self
                        .workers
                        .lock()
                        .expect("Failed to lock workers")
                        .get(&worker_id)
                    {
                        worker.add_active_task(&task);
                    }
                    result.push(MockTaskResultHandle::new(task));
                }
            }
            Ok(result)
        }

        fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId) {
            if let Some(worker) = self
                .workers
                .lock()
                .expect("Failed to lock workers")
                .get(&worker_id)
            {
                worker.mark_task_finished(task_context);
            }
        }

        fn mark_worker_died(&self, worker_id: WorkerId) {
            self.workers
                .lock()
                .expect("Failed to lock workers")
                .remove(&worker_id);
        }

        fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>> {
            Ok(self
                .workers
                .lock()
                .expect("Failed to lock workers")
                .values()
                .map(WorkerSnapshot::from)
                .collect())
        }

        fn try_autoscale(&self, resource_requests: Vec<TaskResourceRequest>) -> DaftResult<()> {
            // Record test-only counters
            {
                let mut calls = self.try_autoscale_calls.lock().expect("lock");
                *calls += 1;
            }
            {
                let mut last_len = self.last_try_autoscale_bundles_len.lock().expect("lock");
                *last_len = Some(resource_requests.len());
            }
            // add 1 worker for each bundle requested to simulate expansion
            let num_new_workers = resource_requests.len();
            let mut workers = self.workers.lock().expect("Failed to lock workers");
            let num_existing_workers = workers.len();
            for i in 0..num_new_workers {
                let new_worker_id: WorkerId =
                    Arc::from(format!("worker{}", num_existing_workers + i + 1));
                workers.insert(
                    new_worker_id.clone(),
                    MockWorker::new(new_worker_id, 1.0, 0.0),
                );
            }
            Ok(())
        }

        fn shutdown(&self) -> DaftResult<()> {
            self.workers
                .lock()
                .expect("Failed to lock workers")
                .values()
                .for_each(|w| w.shutdown());
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    pub struct MockWorker {
        worker_id: WorkerId,
        total_num_cpus: f64,
        total_num_gpus: f64,
        active_task_details: Arc<Mutex<HashMap<TaskContext, TaskDetails>>>,
        #[allow(dead_code)]
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

        pub fn mark_task_finished(&self, task_context: TaskContext) {
            self.active_task_details
                .lock()
                .expect("Failed to lock active_task_details")
                .remove(&task_context);
        }

        pub fn add_active_task(&self, task: &impl Task) {
            self.active_task_details
                .lock()
                .expect("Failed to lock active_task_details")
                .insert(task.task_context(), TaskDetails::from(task));
        }

        #[allow(dead_code)]
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
                .expect("Failed to lock active_task_details")
                .values()
                .map(|details| details.num_cpus())
                .sum()
        }

        fn active_num_gpus(&self) -> f64 {
            self.active_task_details
                .lock()
                .expect("Failed to lock active_task_details")
                .values()
                .map(|details| details.num_gpus())
                .sum()
        }

        fn active_task_details(&self) -> HashMap<TaskContext, TaskDetails> {
            self.active_task_details
                .lock()
                .expect("Failed to lock active_task_details")
                .clone()
        }
    }

    #[test]
    fn test_mock_worker_manager_retire_idle_combined() -> DaftResult<()> {
        use std::collections::HashMap;
        let mut workers = HashMap::new();
        let w1: WorkerId = Arc::from("w1");
        let w2: WorkerId = Arc::from("w2");
        let w3: WorkerId = Arc::from("w3");
        workers.insert(w1.clone(), MockWorker::new(w1.clone(), 1.0, 0.0));
        workers.insert(w2.clone(), MockWorker::new(w2.clone(), 1.0, 0.0));
        workers.insert(w3.clone(), MockWorker::new(w3.clone(), 1.0, 0.0));
        let wm = MockWorkerManager::new(workers);
        // Retire one via wrapper
        let retired = wm.retire_idle_workers(1, false)?;
        assert_eq!(retired, 1);
        // Release all via wrapper (using large max and force_all)
        let released_all = wm.retire_idle_workers(10, true)?;
        assert!(released_all >= 2);
        // Verify unified helper was invoked with expected params
        let calls = wm.idle_calls_log();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, 1);
        assert_eq!(calls[0].1, false);
        assert_eq!(calls[0].2, Some(120));
        assert_eq!(calls[1].1, true);
        Ok(())
    }
}
