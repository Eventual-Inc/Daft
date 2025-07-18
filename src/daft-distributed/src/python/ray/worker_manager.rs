use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, worker::RaySwordfishWorker};
use crate::scheduling::{
    scheduler::WorkerSnapshot,
    task::{SwordfishTask, TaskContext, TaskResourceRequest},
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

        // Try to call start_ray_workers and handle any potential errors
        let ray_workers = match flotilla_module.call_method1(
            pyo3::intern!(py, "start_ray_workers"),
            (workers_guard
                .keys()
                .map(|id| id.as_ref())
                .collect::<Vec<_>>(),),
        ) {
            Ok(result) => match result.extract::<Vec<RaySwordfishWorker>>() {
                Ok(workers) => workers,
                Err(e) => {
                    return Err(common_error::DaftError::InternalError(format!(
                        "Failed to extract Ray workers: {}. Ray may have been shut down",
                        e
                    )));
                }
            },
            Err(e) => {
                // This is where we catch errors from ray.nodes() calls that might fail after Ray shutdown
                return Err(common_error::DaftError::InternalError(format!(
                    "Failed to start Ray workers: {}. Ray may have been shut down",
                    e
                )));
            }
        };

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

    fn try_autoscale(&self, bundles: Vec<TaskResourceRequest>) -> DaftResult<()> {
        let bundles = bundles
            .into_iter()
            .map(|bundle| {
                let mut dict = HashMap::new();
                dict.insert("CPU", bundle.num_cpus().ceil() as i64);
                dict.insert("GPU", bundle.num_gpus().ceil() as i64);
                dict.insert("memory", bundle.memory_bytes() as i64);
                dict
            })
            .collect::<Vec<_>>();
        Python::with_gil(|py| {
            let flotilla_module = match py.import(pyo3::intern!(py, "daft.runners.flotilla")) {
                Ok(module) => module,
                Err(e) => {
                    return Err(common_error::DaftError::InternalError(format!(
                        "Failed to import daft.runners.flotilla for autoscaling: {}. Ray may have been shut down",
                        e
                    )));
                }
            };

            if let Err(e) =
                flotilla_module.call_method1(pyo3::intern!(py, "try_autoscale"), (bundles,))
            {
                return Err(common_error::DaftError::InternalError(format!(
                    "Failed to request autoscaling: {}. Ray may have been shut down",
                    e
                )));
            }

            Ok(())
        })
    }
}
