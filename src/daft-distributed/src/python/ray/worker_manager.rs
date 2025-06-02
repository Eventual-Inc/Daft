use std::collections::HashMap;

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, worker::RaySwordfishWorker};
use crate::scheduling::{
    scheduler::SchedulableTask,
    task::{SwordfishTask, TaskId, TaskResultHandleAwaiter},
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

    fn submit_tasks_to_workers(
        &self,
        tasks_per_worker: HashMap<WorkerId, Vec<SchedulableTask<SwordfishTask>>>,
    ) -> DaftResult<Vec<TaskResultHandleAwaiter<RayTaskResultHandle>>> {
        Python::with_gil(|py| {
            let mut task_result_handles =
                Vec::with_capacity(tasks_per_worker.values().map(|v| v.len()).sum());
            for (worker_id, tasks) in tasks_per_worker {
                let handles = self
                    .ray_workers
                    .get(&worker_id)
                    .expect("Worker should be present in RayWorkerManager")
                    .submit_tasks(tasks, py, &self.task_locals)?;
                task_result_handles.extend(handles);
            }
            DaftResult::Ok(task_result_handles)
        })
    }

    fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
        &self.ray_workers
    }

    fn mark_task_finished(&self, task_id: &TaskId, worker_id: &WorkerId) {
        self.ray_workers
            .get(worker_id)
            .expect("Worker should be present in RayWorkerManager")
            .mark_task_finished(task_id);
    }

    fn total_available_cpus(&self) -> usize {
        self.ray_workers
            .values()
            .map(|w| w.available_num_cpus())
            .sum()
    }

    fn shutdown(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            for worker in self.ray_workers.values() {
                worker.shutdown(py);
            }
        });
        Ok(())
    }
}

impl Drop for RayWorkerManager {
    fn drop(&mut self) {
        self.shutdown().expect("Cannot shutdown RayWorkerManager");
    }
}
