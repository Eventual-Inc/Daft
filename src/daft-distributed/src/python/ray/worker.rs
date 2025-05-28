use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, RaySwordfishTask};
use crate::scheduling::{
    task::{SwordfishTask, TaskDetails, TaskId},
    worker::{Worker, WorkerId},
};

#[pyclass(module = "daft.daft", name = "RaySwordfishWorker")]
#[derive(Debug, Clone)]
pub(crate) struct RaySwordfishWorker {
    worker_id: WorkerId,
    actor_handle: Arc<PyObject>,
    num_cpus: usize,
    #[allow(dead_code)]
    total_memory_bytes: usize,
    active_task_details: Arc<Mutex<HashMap<TaskId, TaskDetails>>>,
}

#[pymethods]
impl RaySwordfishWorker {
    #[new]
    pub fn new(
        worker_id: String,
        actor_handle: PyObject,
        num_cpus: usize,
        total_memory_bytes: usize,
    ) -> Self {
        Self {
            worker_id: Arc::from(worker_id),
            actor_handle: Arc::new(actor_handle),
            num_cpus,
            total_memory_bytes,
            active_task_details: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[allow(dead_code)]
impl RaySwordfishWorker {
    pub fn mark_task_finished(&self, task_id: TaskId) {
        self.active_task_details
            .lock()
            .expect("Active task ids should be present")
            .remove(&task_id);
    }

    pub fn submit_tasks(
        &self,
        tasks: Vec<SwordfishTask>,
        py: Python<'_>,
        task_locals: &pyo3_async_runtimes::TaskLocals,
    ) -> DaftResult<Vec<RayTaskResultHandle>> {
        let (task_details, ray_swordfish_tasks) = tasks
            .into_iter()
            .map(|task| (TaskDetails::from(&task), RaySwordfishTask::new(task)))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let py_task_handles = self
            .actor_handle
            .call_method1(
                py,
                pyo3::intern!(py, "submit_tasks"),
                (ray_swordfish_tasks,),
            )?
            .extract::<Vec<PyObject>>(py)?;

        let task_handles = py_task_handles
            .into_iter()
            .map(|py_task_handle| {
                let task_locals = task_locals.clone_ref(py);
                RayTaskResultHandle::new(py_task_handle, task_locals)
            })
            .collect::<Vec<_>>();

        self.active_task_details
            .lock()
            .expect("Active task ids should be present")
            .extend(
                task_details
                    .into_iter()
                    .map(|details| (details.id.clone(), details)),
            );

        Ok(task_handles)
    }

    pub fn shutdown(&self, py: Python<'_>) {
        self.actor_handle
            .call_method0(py, pyo3::intern!(py, "shutdown"))
            .expect("Failed to shutdown RaySwordfishWorker");
    }
}

impl Worker for RaySwordfishWorker {
    type Task = SwordfishTask;
    type TaskResultHandle = RayTaskResultHandle;

    fn id(&self) -> &WorkerId {
        &self.worker_id
    }

    fn total_num_cpus(&self) -> usize {
        self.num_cpus
    }

    fn active_num_cpus(&self) -> usize {
        self.active_task_details
            .lock()
            .expect("Active task details should be present")
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    fn available_num_cpus(&self) -> usize {
        self.total_num_cpus() - self.active_num_cpus()
    }

    fn active_task_details(&self) -> HashMap<TaskId, TaskDetails> {
        self.active_task_details
            .lock()
            .expect("Active task details should be present")
            .clone()
    }
}
