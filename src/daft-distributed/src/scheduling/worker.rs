use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use common_error::DaftResult;

use super::task::{Task, TaskDetails, TaskResultHandle};
use crate::scheduling::{
    scheduler::WorkerSnapshot,
    task::{TaskContext, TaskResourceRequest},
};

pub(crate) type WorkerId = Arc<str>;

/// Identifies one owner of autoscaling demand: in practice one scheduler event loop,
/// i.e. one running plan.
///
/// Backends whose autoscaler exposes a single, replace-on-write demand slot (Ray's
/// `ray.autoscaler.sdk.request_resources` is exactly that) cannot simply forward the
/// latest caller's request: doing so silently discards the demand of every other plan
/// running against the same cluster. Tagging each request with its owner lets the
/// backend keep per-owner books and always publish the union of what is still live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct AutoscaleDemandId(u64);

impl AutoscaleDemandId {
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for AutoscaleDemandId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

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

#[derive(Default)]
pub(crate) struct DispatchLifecycleGuard<'a> {
    _lock: Option<std::sync::MutexGuard<'a, ()>>,
}

impl<'a> DispatchLifecycleGuard<'a> {
    pub(crate) fn new(lock: std::sync::MutexGuard<'a, ()>) -> Self {
        Self { _lock: Some(lock) }
    }
}

pub(crate) trait WorkerManager: Send + Sync {
    type Worker: Worker;

    fn begin_dispatch(&self) -> DispatchLifecycleGuard<'_> {
        DispatchLifecycleGuard::default()
    }

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<<<Self as WorkerManager>::Worker as Worker>::Task>>,
    ) -> DaftResult<Vec<<<Self as WorkerManager>::Worker as Worker>::TaskResultHandle>>;
    fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId);
    fn mark_worker_died(&self, worker_id: WorkerId);
    fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>>;
    /// Signal scale-up demand on behalf of `demand_id`.
    ///
    /// Backends with a single, replace-on-write demand slot must publish the union of
    /// the demand currently held by *all* owners, not just this one — otherwise two
    /// concurrent plans keep cancelling each other's requests.
    fn try_autoscale(
        &self,
        demand_id: AutoscaleDemandId,
        resource_requests: Vec<TaskResourceRequest>,
    ) -> DaftResult<()>;
    fn cleanup_shuffle_dirs(
        &self,
        _dirs: Vec<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DaftResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
    #[allow(dead_code)]
    fn shutdown(&self) -> DaftResult<()>;
    /// Drop the autoscaling (scale-up) demand previously signaled by `demand_id`. The
    /// scheduler calls this (best-effort) whenever a job finishes — successfully or
    /// with an error — so the autoscaler stops provisioning capacity for work that no
    /// longer exists.
    ///
    /// Only this owner's demand is released: any demand still held by other concurrent
    /// plans must survive, so backends that share one demand slot re-publish the
    /// remainder instead of clearing the slot outright.
    ///
    /// Idle-worker *retirement* is intentionally NOT triggered here. Retirement is
    /// owned solely by the worker manager's own background reaper, which lives across
    /// query boundaries and is the single retirement authority (see RayWorkerManager).
    /// Keeping the scheduler out of the retirement decision avoids two independent
    /// actors racing over the same worker pool. The default implementation is a no-op
    /// for backends without an autoscaler.
    fn clear_autoscale_demand(&self, _demand_id: AutoscaleDemandId) -> DaftResult<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Mutex, TryLockError,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use super::*;
    use crate::scheduling::tests::{MockTask, MockTaskResultHandle};

    /// A mock implementation of the WorkerManager trait for testing
    #[derive(Clone)]
    pub struct MockWorkerManager {
        workers: Arc<Mutex<HashMap<WorkerId, MockWorker>>>,
        pub dispatch_gate: Arc<Mutex<()>>,
        check_dispatch_gate: Arc<AtomicBool>,
        clear_demand_call_count: Arc<AtomicUsize>,
        fail_worker_snapshots: Arc<AtomicBool>,
        fail_autoscale: Arc<AtomicBool>,
        fail_submit: Arc<AtomicBool>,
        fail_clear_demand: Arc<AtomicBool>,
        autoscale_creates_workers: Arc<AtomicBool>,
        after_snapshot_hook: Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>,
        active_demand_ids: Arc<Mutex<HashSet<AutoscaleDemandId>>>,
        /// Owners seen by `try_autoscale`, in call order.
        autoscale_demand_ids: Arc<Mutex<Vec<AutoscaleDemandId>>>,
        /// Owners seen by `clear_autoscale_demand`, in call order.
        cleared_demand_ids: Arc<Mutex<Vec<AutoscaleDemandId>>>,
    }

    impl MockWorkerManager {
        pub fn new(workers: HashMap<WorkerId, MockWorker>) -> Self {
            Self {
                workers: Arc::new(Mutex::new(workers)),
                dispatch_gate: Arc::new(Mutex::new(())),
                check_dispatch_gate: Arc::new(AtomicBool::new(false)),
                clear_demand_call_count: Arc::new(AtomicUsize::new(0)),
                fail_worker_snapshots: Arc::new(AtomicBool::new(false)),
                fail_autoscale: Arc::new(AtomicBool::new(false)),
                fail_submit: Arc::new(AtomicBool::new(false)),
                fail_clear_demand: Arc::new(AtomicBool::new(false)),
                autoscale_creates_workers: Arc::new(AtomicBool::new(true)),
                after_snapshot_hook: Arc::new(Mutex::new(None)),
                active_demand_ids: Arc::new(Mutex::new(HashSet::new())),
                autoscale_demand_ids: Arc::new(Mutex::new(Vec::new())),
                cleared_demand_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn clear_demand_call_count(&self) -> usize {
            self.clear_demand_call_count.load(Ordering::SeqCst)
        }

        pub fn autoscale_demand_ids(&self) -> Vec<AutoscaleDemandId> {
            self.autoscale_demand_ids
                .lock()
                .expect("Failed to lock autoscale_demand_ids")
                .clone()
        }

        pub fn cleared_demand_ids(&self) -> Vec<AutoscaleDemandId> {
            self.cleared_demand_ids
                .lock()
                .expect("Failed to lock cleared_demand_ids")
                .clone()
        }

        /// Make subsequent `worker_snapshots` calls fail, to exercise the scheduler's
        /// error-exit path.
        pub fn set_fail_worker_snapshots(&self, fail: bool) {
            self.fail_worker_snapshots.store(fail, Ordering::SeqCst);
        }

        pub fn set_fail_autoscale(&self, fail: bool) {
            self.fail_autoscale.store(fail, Ordering::SeqCst);
        }

        pub fn set_fail_submit(&self, fail: bool) {
            self.fail_submit.store(fail, Ordering::SeqCst);
        }

        pub fn set_fail_clear_demand(&self, fail: bool) {
            self.fail_clear_demand.store(fail, Ordering::SeqCst);
        }

        pub fn set_autoscale_creates_workers(&self, enabled: bool) {
            self.autoscale_creates_workers
                .store(enabled, Ordering::SeqCst);
        }

        pub fn active_demand_ids(&self) -> HashSet<AutoscaleDemandId> {
            self.active_demand_ids.lock().unwrap().clone()
        }

        // Dispatcher-only tests intentionally submit without the scheduler's gate.
        pub fn enable_dispatch_gate_checks(&self) {
            self.check_dispatch_gate.store(true, Ordering::SeqCst);
        }

        fn assert_dispatch_gate_held(&self) {
            if self.check_dispatch_gate.load(Ordering::SeqCst) {
                assert!(
                    matches!(self.dispatch_gate.try_lock(), Err(TryLockError::WouldBlock)),
                    "scheduler must hold the dispatch gate"
                );
            }
        }

        pub fn set_after_snapshot_hook(&self, hook: impl FnOnce() + Send + 'static) {
            *self.after_snapshot_hook.lock().unwrap() = Some(Box::new(hook));
        }

        // Model a reaper using the same lifecycle gate and checking for active work.
        pub fn try_reap_idle_worker(&self, worker_id: &WorkerId) -> bool {
            let _guard = match self.dispatch_gate.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => return false,
                Err(TryLockError::Poisoned(_)) => panic!("dispatch gate poisoned"),
            };
            let mut workers = self.workers.lock().unwrap();
            if workers
                .get(worker_id)
                .is_some_and(|worker| worker.active_task_details().is_empty())
            {
                workers.remove(worker_id);
                true
            } else {
                false
            }
        }
    }

    impl WorkerManager for MockWorkerManager {
        type Worker = MockWorker;

        fn begin_dispatch(&self) -> DispatchLifecycleGuard<'_> {
            DispatchLifecycleGuard::new(self.dispatch_gate.lock().unwrap())
        }

        fn submit_tasks_to_workers(
            &self,
            tasks_per_worker: HashMap<WorkerId, Vec<MockTask>>,
        ) -> DaftResult<Vec<MockTaskResultHandle>> {
            self.assert_dispatch_gate_held();
            if self.fail_submit.load(Ordering::SeqCst) {
                return Err(common_error::DaftError::InternalError(
                    "injected submit failure".to_string(),
                ));
            }
            let mut result = Vec::new();
            for (worker_id, tasks) in tasks_per_worker {
                for task in tasks {
                    // Update the worker's active task count
                    let workers = self.workers.lock().expect("Failed to lock workers");
                    let worker = workers.get(&worker_id);
                    if self.check_dispatch_gate.load(Ordering::SeqCst) {
                        assert!(
                            worker.is_some(),
                            "worker disappeared between snapshot and submit"
                        );
                    }
                    if let Some(worker) = worker {
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
            self.assert_dispatch_gate_held();
            if self.fail_worker_snapshots.load(Ordering::SeqCst) {
                return Err(common_error::DaftError::InternalError(
                    "injected worker_snapshots failure".to_string(),
                ));
            }
            let snapshots = self
                .workers
                .lock()
                .expect("Failed to lock workers")
                .values()
                .map(WorkerSnapshot::from)
                .collect();
            // Release the worker-map lock before allowing the reaper to run. Only
            // the scheduler's lifecycle gate should protect these snapshots now.
            let hook = self.after_snapshot_hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
            Ok(snapshots)
        }

        fn try_autoscale(
            &self,
            demand_id: AutoscaleDemandId,
            resource_requests: Vec<TaskResourceRequest>,
        ) -> DaftResult<()> {
            self.assert_dispatch_gate_held();
            self.autoscale_demand_ids
                .lock()
                .expect("Failed to lock autoscale_demand_ids")
                .push(demand_id);
            if self.fail_autoscale.load(Ordering::SeqCst) {
                return Err(common_error::DaftError::InternalError(
                    "injected autoscale failure".to_string(),
                ));
            }
            self.active_demand_ids.lock().unwrap().insert(demand_id);
            if !self.autoscale_creates_workers.load(Ordering::SeqCst) {
                return Ok(());
            }
            // add 1 worker for each num_cpus
            let num_workers = resource_requests.len();
            let mut workers = self.workers.lock().expect("Failed to lock workers");
            let num_existing_workers = workers.len();
            for i in 0..num_workers {
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

        fn clear_autoscale_demand(&self, demand_id: AutoscaleDemandId) -> DaftResult<()> {
            // Mock implementation: distributed Ray autoscaler is not exercised in unit tests.
            if self.check_dispatch_gate.load(Ordering::SeqCst) {
                assert!(
                    self.dispatch_gate.try_lock().is_ok(),
                    "dispatch gate must be released before demand cleanup"
                );
            }
            self.cleared_demand_ids
                .lock()
                .expect("Failed to lock cleared_demand_ids")
                .push(demand_id);
            self.clear_demand_call_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_clear_demand.load(Ordering::SeqCst) {
                return Err(common_error::DaftError::InternalError(
                    "injected clear_autoscale_demand failure".to_string(),
                ));
            }
            self.active_demand_ids.lock().unwrap().remove(&demand_id);
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
}
