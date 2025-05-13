use std::{collections::HashMap, sync::Arc};

use common_daft_config::{DaftExecutionConfig, PyDaftExecutionConfig};
use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::*, RaySwordfishWorker};
use crate::scheduling::{
    task::{SwordfishTask, SwordfishTaskResultHandle, Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager, WorkerManagerFactory},
};

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
#[allow(dead_code)]
pub(crate) struct RayWorkerManager {
    ray_workers: HashMap<WorkerId, RaySwordfishWorker>,
    task_locals: pyo3_async_runtimes::TaskLocals,
}

// TODO(FLOTILLA_MS1): Make Ray worker manager live for the duration of the program
// so that we don't have to recreate it on every stage.
impl RayWorkerManager {
    pub fn try_new(task_locals: &pyo3_async_runtimes::TaskLocals) -> DaftResult<Self> {
        let (ray_workers, task_locals) = Python::with_gil(|py| {
            let distributed_swordfish_module = py.import("daft.runners.distributed_swordfish")?;
            let ray_workers = distributed_swordfish_module
                .call_method0("start_ray_workers")?
                .extract::<Vec<RaySwordfishWorker>>()?;
            let ray_worker_hashmap = ray_workers
                .into_iter()
                .map(|w| (w.id().clone(), w))
                .collect();
            let task_locals = task_locals.clone_ref(py);
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
        &mut self,
        task: Box<dyn Task>,
        worker_id: String,
    ) -> Box<dyn SwordfishTaskResultHandle> {
        self.ray_workers
            .get_mut(&worker_id)
            .unwrap()
            .submit_task(task, &self.task_locals)
            .expect("Failed to submit task to RayWorkerManager")
    }

    fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
        &self.ray_workers
    }

    fn mark_task_finished(&mut self, task_id: TaskId, worker_id: WorkerId) {
        self.ray_workers
            .get_mut(&worker_id)
            .unwrap()
            .mark_task_finished(task_id);
    }

    fn total_available_cpus(&self) -> usize {
        self.ray_workers
            .values()
            .map(|w| w.num_cpus() - w.active_task_ids().len())
            .sum()
    }

    fn shutdown(&mut self) -> DaftResult<()> {
        for worker in self.ray_workers.values_mut() {
            worker.shutdown();
        }
        Ok(())
    }
}

pub(crate) struct RayWorkerManagerFactory {
    daft_execution_config: Arc<DaftExecutionConfig>,
    task_locals: pyo3_async_runtimes::TaskLocals,
}

impl RayWorkerManagerFactory {
    pub fn new(
        daft_execution_config: Arc<DaftExecutionConfig>,
        task_locals: pyo3_async_runtimes::TaskLocals,
    ) -> Self {
        Self {
            daft_execution_config,
            task_locals,
        }
    }
}

impl WorkerManagerFactory for RayWorkerManagerFactory {
    type WorkerManager = RayWorkerManager;

    fn create_worker_manager(&self) -> DaftResult<Self::WorkerManager> {
        RayWorkerManager::try_new(&self.task_locals)
    }
}
