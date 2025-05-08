use std::sync::Arc;

use common_daft_config::{DaftExecutionConfig, PyDaftExecutionConfig};
use common_error::DaftResult;
use pyo3::prelude::*;

use super::task::*;
use crate::scheduling::{
    task::{SwordfishTask, SwordfishTaskResultHandle},
    worker::{WorkerManager, WorkerManagerFactory},
};

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
#[allow(dead_code)]
pub(crate) struct RayWorkerManager {
    ray_worker_manager: PyObject,
    task_locals: pyo3_async_runtimes::TaskLocals,
}

// TODO(FLOTILLA_MS1): Make Ray worker manager live for the duration of the program
// so that we don't have to recreate it on every stage.
impl RayWorkerManager {
    pub fn try_new(
        daft_execution_config: Arc<DaftExecutionConfig>,
        task_locals: &pyo3_async_runtimes::TaskLocals,
    ) -> DaftResult<Self> {
        let py_daft_execution_config = PyDaftExecutionConfig {
            config: daft_execution_config,
        };
        let (ray_worker_manager, task_locals) = Python::with_gil(|py| {
            let distributed_swordfish_module = py.import("daft.runners.distributed_swordfish")?;
            let ray_worker_manager_class =
                distributed_swordfish_module.getattr("RaySwordfishWorkerManager")?;
            let instance = ray_worker_manager_class.call1((py_daft_execution_config,))?;
            let task_locals = task_locals.clone_ref(py);
            DaftResult::Ok((instance.unbind(), task_locals))
        })?;
        Ok(Self {
            ray_worker_manager,
            task_locals,
        })
    }
}

impl WorkerManager for RayWorkerManager {
    fn submit_task_to_worker(
        &self,
        task: SwordfishTask,
        worker_id: String,
    ) -> Box<dyn SwordfishTaskResultHandle> {
        Python::with_gil(|py| {
            let py_task = RaySwordfishTask::new(task);
            let py_task_handle = self
                .ray_worker_manager
                .call_method1(
                    py,
                    pyo3::intern!(py, "submit_task_to_worker"),
                    (py_task, worker_id),
                )
                .expect("Failed to submit task to RayWorkerManager");
            let task_locals = self.task_locals.clone_ref(py);
            Box::new(RayTaskResultHandle::new(py_task_handle, task_locals))
        })
    }

    fn get_worker_resources(&self) -> Vec<(String, usize, usize)> {
        Python::with_gil(|py| {
            let py_worker_resources = self
                .ray_worker_manager
                .call_method0(py, pyo3::intern!(py, "get_worker_resources"))
                .expect("Failed to get worker resources from RayWorkerManager");
            py_worker_resources
                .extract::<Vec<(String, usize, usize)>>(py)
                .expect("Failed to extract worker resources from RayWorkerManager")
        })
    }

    fn try_autoscale(&self, num_workers: usize) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.ray_worker_manager.call_method1(
                py,
                pyo3::intern!(py, "try_autoscale"),
                (num_workers,),
            )
        })?;
        Ok(())
    }

    fn shutdown(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.ray_worker_manager
                .call_method0(py, pyo3::intern!(py, "shutdown"))
        })?;
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
    fn create_worker_manager(&self) -> DaftResult<Box<dyn WorkerManager>> {
        RayWorkerManager::try_new(self.daft_execution_config.clone(), &self.task_locals)
            .map(|ray_worker_manager| Box::new(ray_worker_manager) as Box<dyn WorkerManager>)
    }
}
