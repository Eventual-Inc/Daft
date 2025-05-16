use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use futures::io::Take;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, RaySwordfishTask};
use crate::scheduling::{
    task::{SwordfishTask, SwordfishTaskResultHandle, Task, TaskId},
    worker::{Worker, WorkerId},
};

#[pyclass(module = "daft.daft", name = "RaySwordfishWorker")]
#[derive(Debug, Clone)]
pub(crate) struct RaySwordfishWorker {
    worker_id: WorkerId,
    actor_handle: Arc<PyObject>,
    num_cpus: usize,
    total_memory_bytes: usize,
    active_task_ids: Arc<Mutex<HashSet<String>>>,
}

#[pymethods]
impl RaySwordfishWorker {
    #[new]
    pub fn new(
        worker_id: WorkerId,
        actor_handle: PyObject,
        num_cpus: usize,
        total_memory_bytes: usize,
    ) -> Self {
        Self {
            worker_id,
            actor_handle: Arc::new(actor_handle),
            num_cpus,
            total_memory_bytes,
            active_task_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl RaySwordfishWorker {
    pub fn mark_task_finished(&self, task_id: String) {
        self.active_task_ids.lock().unwrap().remove(&task_id);
    }

    pub fn submit_task(
        &self,
        task: Box<dyn Task>,
        task_locals: &pyo3_async_runtimes::TaskLocals,
    ) -> DaftResult<Box<dyn SwordfishTaskResultHandle>> {
        let task = task.into_any().downcast::<SwordfishTask>().unwrap();
        let task_id = task.task_id().to_string();
        let py_task = RaySwordfishTask::new(task);
        let py_task_handle = Python::with_gil(|py| {
            let py_task_handle = self
                .actor_handle
                .call_method1(py, pyo3::intern!(py, "submit_task"), (py_task,))
                .expect("Failed to submit task to RayWorker");
            let task_locals = task_locals.clone_ref(py);
            Box::new(RayTaskResultHandle::new(
                py_task_handle,
                task_locals,
                self.worker_id.clone(),
            ))
        });
        self.active_task_ids.lock().unwrap().insert(task_id);
        Ok(py_task_handle)
    }

    pub fn shutdown(&self) {
        Python::with_gil(|py| {
            self.actor_handle
                .call_method0(py, pyo3::intern!(py, "shutdown"))
                .unwrap();
        });
    }
}

impl Worker for RaySwordfishWorker {
    fn id(&self) -> &WorkerId {
        &self.worker_id
    }

    fn num_cpus(&self) -> usize {
        self.num_cpus
    }

    fn active_task_ids(&self) -> HashSet<TaskId> {
        self.active_task_ids.lock().unwrap().clone()
    }
}
