use std::sync::Arc;

use common_daft_config::{DaftExecutionConfig, PyDaftExecutionConfig};
use common_error::DaftResult;
use pyo3::prelude::*;

use super::task::*;
use crate::scheduling::{
    task::{SwordfishTask, SwordfishTaskResultHandle},
    worker::WorkerManager,
};

// Wrapper around the RaySwordfishWorkerManager class in the distributed_swordfish module.
#[allow(dead_code)]
pub(crate) struct RayWorkerManager {
    ray_worker_manager: PyObject,
}

impl RayWorkerManager {
    pub fn try_new(daft_execution_config: Arc<DaftExecutionConfig>) -> DaftResult<Self> {
        let py_daft_execution_config = PyDaftExecutionConfig {
            config: daft_execution_config,
        };
        let ray_worker_manager = Python::with_gil(|py| {
            let distributed_swordfish_module = py.import("daft.runners.distributed_swordfish")?;
            let ray_worker_manager_class =
                distributed_swordfish_module.getattr("RaySwordfishWorkerManager")?;
            let instance = ray_worker_manager_class.call1((py_daft_execution_config,))?;

            DaftResult::Ok(instance.unbind())
        })?;
        Ok(Self { ray_worker_manager })
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
            Box::new(RayTaskResultHandle::new(py_task_handle))
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
