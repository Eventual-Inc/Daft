use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, RaySwordfishTask};
use crate::scheduling::{
    task::{SwordfishTask, Task, TaskId},
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
    active_task_ids: Arc<Mutex<HashSet<TaskId>>>,
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
            active_task_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[allow(dead_code)]
impl RaySwordfishWorker {
    pub fn mark_task_finished(&self, task_id: TaskId) {
        self.active_task_ids
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
        let (task_ids, tasks): (Vec<TaskId>, Vec<RaySwordfishTask>) = tasks
            .into_iter()
            .map(|task| (task.task_id().clone(), RaySwordfishTask::new(task)))
            .unzip();

        let py_task_handles = self
            .actor_handle
            .call_method1(py, pyo3::intern!(py, "submit_tasks"), (tasks,))?
            .extract::<Vec<PyObject>>(py)?;

        let task_handles = py_task_handles
            .into_iter()
            .map(|py_task_handle| {
                let task_locals = task_locals.clone_ref(py);
                RayTaskResultHandle::new(py_task_handle, task_locals)
            })
            .collect::<Vec<_>>();

        self.active_task_ids
            .lock()
            .expect("Active task ids should be present")
            .extend(task_ids);

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

    fn num_cpus(&self) -> usize {
        self.num_cpus
    }

    fn active_task_ids(&self) -> HashSet<TaskId> {
        self.active_task_ids
            .lock()
            .expect("Active task ids should be present")
            .clone()
    }
}
