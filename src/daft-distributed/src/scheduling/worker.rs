use std::{collections::HashMap, fmt::Debug, sync::Arc};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::task::{Task, TaskDetails, TaskResultHandle};
use crate::scheduling::{
    scheduler::WorkerSnapshot,
    task::{TaskContext, TaskResourceRequest},
};

pub(crate) type WorkerId = Arc<str>;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Number of worker replicas with this configuration
    pub num_replicas: usize,
    /// Number of CPUs per worker
    pub num_cpus: f64,
    /// Memory in bytes per worker
    pub memory_bytes: usize,
    /// Number of GPUs per worker
    pub num_gpus: f64,
}

impl WorkerConfig {
    /// Creates a new WorkerConfig with validation.
    pub fn new(num_replicas: usize, num_cpus: f64, memory_bytes: usize, num_gpus: f64) -> Result<Self, String> {
        if num_replicas == 0 {
            return Err(format!("num_replicas must be positive, got {}", num_replicas));
        }
        if num_cpus <= 0.0 {
            return Err(format!("num_cpus must be positive, got {}", num_cpus));
        }
        if memory_bytes == 0 {
            return Err(format!("memory_bytes must be positive, got {}", memory_bytes));
        }
        if num_gpus < 0.0 {
            return Err(format!("num_gpus cannot be negative, got {}", num_gpus));
        }

        Ok(Self {
            num_replicas,
            num_cpus,
            memory_bytes,
            num_gpus,
        })
    }
}

#[pyclass(module = "daft.daft", name = "WorkerConfig")]
#[derive(Debug, Clone)]
pub struct PyWorkerConfig {
    inner: WorkerConfig,
}

#[pymethods]
impl PyWorkerConfig {
    #[new]
    #[pyo3(signature = (num_replicas, num_cpus, memory_bytes, num_gpus=0.0))]
    pub fn new(num_replicas: usize, num_cpus: f64, memory_bytes: usize, num_gpus: f64) -> PyResult<Self> {
        let inner = WorkerConfig::new(num_replicas, num_cpus, memory_bytes, num_gpus)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;
        Ok(Self { inner })
    }

    #[getter]
    fn num_replicas(&self) -> usize {
        self.inner.num_replicas
    }

    #[getter]
    fn num_cpus(&self) -> f64 {
        self.inner.num_cpus
    }

    #[getter]
    fn memory_bytes(&self) -> usize {
        self.inner.memory_bytes
    }

    #[getter]
    fn num_gpus(&self) -> f64 {
        self.inner.num_gpus
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkerConfig(num_replicas={}, num_cpus={}, memory_bytes={}, num_gpus={})",
            self.inner.num_replicas, self.inner.num_cpus, self.inner.memory_bytes, self.inner.num_gpus
        )
    }
}

impl From<PyWorkerConfig> for WorkerConfig {
    fn from(py_config: PyWorkerConfig) -> Self {
        py_config.inner
    }
}

impl From<&PyWorkerConfig> for WorkerConfig {
    fn from(py_config: &PyWorkerConfig) -> Self {
        py_config.inner.clone()
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
    }

    impl MockWorkerManager {
        pub fn new(workers: HashMap<WorkerId, MockWorker>) -> Self {
            Self {
                workers: Arc::new(Mutex::new(workers)),
            }
        }
    }

    impl WorkerManager for MockWorkerManager {
        type Worker = MockWorker;

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
