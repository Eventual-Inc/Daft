use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, worker::RaySwordfishWorker};
use crate::scheduling::{
    scheduler::WorkerSnapshot,
    task::{SwordfishTask, TaskContext},
    worker::{Worker, WorkerId, WorkerManager},
};

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
pub(crate) struct RayWorkerManager {
    ray_workers: Arc<Mutex<HashMap<WorkerId, RaySwordfishWorker>>>,
    task_locals: pyo3_async_runtimes::TaskLocals,
}

impl RayWorkerManager {
    pub fn try_new(py: Python) -> DaftResult<Self> {
        let task_locals = pyo3_async_runtimes::tokio::get_current_locals(py)
            .expect("Failed to get current task locals");

        Ok(Self {
            ray_workers: Arc::new(Mutex::new(HashMap::new())),
            task_locals,
        })
    }

    fn refresh_workers(&self, py: Python) -> DaftResult<()> {
        let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

        // Get existing worker IDs to avoid duplicates
        let mut workers_guard = self
            .ray_workers
            .lock()
            .expect("Failed to lock RayWorkerManager");

        let ray_workers = flotilla_module
            .call_method1(
                pyo3::intern!(py, "start_ray_workers"),
                (workers_guard
                    .keys()
                    .map(|id| id.as_ref())
                    .collect::<Vec<_>>(),),
            )?
            .extract::<Vec<RaySwordfishWorker>>()?;

        for worker in ray_workers {
            workers_guard.insert(worker.id().clone(), worker);
        }

        DaftResult::Ok(())
    }
}

impl WorkerManager for RayWorkerManager {
    type Worker = RaySwordfishWorker;

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<SwordfishTask>>,
    ) -> DaftResult<Vec<RayTaskResultHandle>> {
        Python::with_gil(|py| {
            // Refresh workers before submitting tasks to ensure we have the latest workers
            self.refresh_workers(py)?;
            let mut task_result_handles =
                Vec::with_capacity(tasks_per_worker.values().map(|v| v.len()).sum());

            let mut workers = self
                .ray_workers
                .lock()
                .expect("Failed to lock RayWorkerManager");
            for (worker_id, tasks) in tasks_per_worker {
                let handles = workers
                    .get_mut(&worker_id)
                    .expect("Worker should be present in RayWorkerManager")
                    .submit_tasks(tasks, py, &self.task_locals)?;
                task_result_handles.extend(handles);
            }
            DaftResult::Ok(task_result_handles)
        })
    }

    fn worker_snapshots(&self) -> DaftResult<Vec<WorkerSnapshot>> {
        Python::with_gil(|py| self.refresh_workers(py))?;
        let workers_guard = self
            .ray_workers
            .lock()
            .expect("Failed to lock RayWorkerManager");
        Ok(workers_guard
            .values()
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>())
    }

    fn mark_task_finished(&self, task_context: TaskContext, worker_id: WorkerId) {
        let mut workers = self
            .ray_workers
            .lock()
            .expect("Failed to lock RayWorkerManager");
        workers
            .get_mut(&worker_id)
            .expect("Worker should be present in RayWorkerManager")
            .mark_task_finished(&task_context);
    }

    fn mark_worker_died(&self, worker_id: WorkerId) {
        let mut workers_guard = self
            .ray_workers
            .lock()
            .expect("Failed to lock RayWorkerManager");
        workers_guard.remove(&worker_id);
    }

    fn shutdown(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let workers_guard = self
                .ray_workers
                .lock()
                .expect("Failed to lock RayWorkerManager");
            for worker in workers_guard.values() {
                worker.shutdown(py);
            }
        });
        Ok(())
    }

    fn try_autoscale(&self, num_cpus: usize) -> DaftResult<()> {
        Python::with_gil(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;
            flotilla_module.call_method1(pyo3::intern!(py, "try_autoscale"), (num_cpus,))?;
            Ok(())
        })
    }
}

impl Drop for RayWorkerManager {
    fn drop(&mut self) {
        self.shutdown().expect("Cannot shutdown RayWorkerManager");
    }
}
