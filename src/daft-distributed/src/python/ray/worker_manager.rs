use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::worker::RaySwordfishWorker;
use crate::scheduling::{
    task::{SwordfishTaskResultHandle, Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager},
};

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
#[allow(dead_code)]
pub(crate) struct RayWorkerManager {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    task_locals: pyo3_async_runtimes::TaskLocals,
}

impl RayWorkerManager {
    pub fn try_new() -> DaftResult<Self> {
        let (ray_workers, task_locals) = Python::with_gil(|py| {
            let distributed_swordfish_module = py.import("daft.runners.distributed_swordfish")?;
            let ray_workers = distributed_swordfish_module
                .call_method0("start_ray_workers")?
                .extract::<Vec<RaySwordfishWorker>>()?;
            let ray_worker_hashmap = ray_workers
                .into_iter()
                .map(|w| (w.id().clone(), w))
                .collect();
            let task_locals = pyo3_async_runtimes::tokio::get_current_locals(py)
                .expect("Failed to get current task locals");
            DaftResult::Ok((ray_worker_hashmap, task_locals))
        })?;
        Ok(Self {
            ray_workers,
            task_locals,
        })
    }
}

impl WorkerManager for RayWorkerManager {
    type Worker = RaySwordfishWorker;

    fn submit_task_to_worker(
        &self,
        task: Box<dyn Task>,
        worker_id: WorkerId,
    ) -> Box<dyn SwordfishTaskResultHandle> {
        self.ray_workers
            .get(&worker_id)
            .expect("Worker should be present in RayWorkerManager")
            .submit_task(task, &self.task_locals)
            .expect("Failed to submit task to RayWorkerManager")
    }

    fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
        &self.ray_workers
    }

    fn mark_task_finished(&self, task_id: TaskId, worker_id: WorkerId) {
        self.ray_workers
            .get(&worker_id)
            .expect("Worker should be present in RayWorkerManager")
            .mark_task_finished(task_id);
    }

    fn total_available_cpus(&self) -> usize {
        self.ray_workers
            .values()
            .map(|w| w.num_cpus() - w.active_task_ids().len())
            .sum()
    }

    fn shutdown(&self) -> DaftResult<()> {
        for worker in self.ray_workers.values() {
            worker.shutdown();
        }
        Ok(())
    }
}

impl Drop for RayWorkerManager {
    fn drop(&mut self) {
        self.shutdown().expect("Cannot shutdown RayWorkerManager");
    }
}

#[pyclass(module = "daft.daft", name = "RayWorkerManager")]
#[derive(Clone)]
pub(crate) struct PyRayWorkerManager {
    pub inner: Arc<RayWorkerManager>,
}

#[pymethods]
impl PyRayWorkerManager {
    #[new]
    pub fn new() -> DaftResult<Self> {
        let inner = RayWorkerManager::try_new()?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

impl PyRayWorkerManager {
    pub fn inner(&self) -> Arc<dyn WorkerManager<Worker = RaySwordfishWorker>> {
        self.inner.clone()
    }
}
