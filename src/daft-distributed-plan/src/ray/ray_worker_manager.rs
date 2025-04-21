use pyo3::prelude::*;

use super::ray_task_handle::RayTaskHandle;
use crate::{
    python::PySwordfishWorkerTask,
    task::{Task, TaskHandle},
    worker::WorkerManager,
};

pub(crate) struct RayWorkerManager {
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
    fn submit_task_to_worker(&self, task: Task, worker_id: String) -> TaskHandle {
        Python::with_gil(|py| {
            let py_task = PySwordfishWorkerTask { task };
            let py_task_handle = self
                .ray_worker_manager
                .call_method1(
                    py,
                    pyo3::intern!(py, "submit_task_to_worker"),
                    (py_task, worker_id),
                )
                .unwrap();
            TaskHandle::Ray(RayTaskHandle::new(py_task_handle))
        })
    }

    fn get_worker_resources(&self) -> Vec<(String, usize, usize)> {
        Python::with_gil(|py| {
            let py_worker_resources = self
                .ray_worker_manager
                .call_method0(py, pyo3::intern!(py, "get_worker_resources"))
                .unwrap();
            let py_worker_resources = py_worker_resources
                .extract::<Vec<(String, usize, usize)>>(py)
                .unwrap();
            py_worker_resources
        })
    }

    fn try_autoscale(&self, num_workers: usize) -> () {
        Python::with_gil(|py| {
            self.ray_worker_manager
                .call_method1(py, pyo3::intern!(py, "try_autoscale"), (num_workers,))
                .unwrap();
            ()
        })
    }
}
