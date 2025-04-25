use pyo3::prelude::*;

use super::task::{RaySwordfishTask, RayTaskResultHandle};
use crate::scheduling::{
    task::{SwordfishTask, SwordfishTaskResultHandle},
    worker::WorkerManager,
};

pub struct RayWorkerManager {
    ray_worker_manager: PyObject,
}

impl RayWorkerManager {
    pub fn new() -> Self {
        Python::with_gil(|py| {
            let distributed_swordfish_module =
                py.import("daft.runners.distributed_swordfish").unwrap();
            let ray_worker_manager_class = distributed_swordfish_module
                .getattr("RaySwordfishWorkerManager")
                .unwrap();
            let instance = ray_worker_manager_class.call0().unwrap();

            Self {
                ray_worker_manager: instance.into(),
            }
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
            let py_task = RaySwordfishTask { task };
            let py_task_handle = self
                .ray_worker_manager
                .call_method1(
                    py,
                    pyo3::intern!(py, "submit_task_to_worker"),
                    (py_task, worker_id),
                )
                .unwrap();
            Box::new(RayTaskResultHandle::new(py_task_handle))
        })
    }

    fn get_worker_resources(&self) -> Vec<(String, usize, usize)> {
        Python::with_gil(|py| {
            let py_worker_resources = self
                .ray_worker_manager
                .call_method0(py, pyo3::intern!(py, "get_worker_resources"))
                .unwrap();
            py_worker_resources
                .extract::<Vec<(String, usize, usize)>>(py)
                .unwrap()
        })
    }

    fn try_autoscale(&self, num_workers: usize) {
        Python::with_gil(|py| {
            self.ray_worker_manager
                .call_method1(py, pyo3::intern!(py, "try_autoscale"), (num_workers,))
                .unwrap();
        });
    }

    fn shutdown(&self) {
        Python::with_gil(|py| {
            self.ray_worker_manager
                .call_method0(py, pyo3::intern!(py, "shutdown"))
                .unwrap();
        });
    }
}
