use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, RaySwordfishTask};
use crate::scheduling::{
    scheduler::SchedulableTask,
    task::{SwordfishTask, Task, TaskId, TaskResultHandleAwaiter},
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
    pub fn mark_task_finished(&self, task_id: &TaskId) {
        self.active_task_ids
            .lock()
            .expect("Active task ids should be present")
            .remove(task_id);
    }

    pub fn submit_tasks(
        &self,
        tasks: Vec<SchedulableTask<SwordfishTask>>,
        py: Python<'_>,
        task_locals: &pyo3_async_runtimes::TaskLocals,
    ) -> DaftResult<Vec<TaskResultHandleAwaiter<RayTaskResultHandle>>> {
        let mut task_handles = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (task, result_tx, cancel_token) = task.into_inner();
            let task_id = task.task_id().clone();

            let ray_swordfish_task = RaySwordfishTask::new(task);
            let py_task_handle = self.actor_handle.call_method1(
                py,
                pyo3::intern!(py, "submit_task"),
                (ray_swordfish_task,),
            )?;

            self.active_task_ids
                .lock()
                .expect("Active task ids should be present")
                .insert(task_id.clone());

            let task_locals = task_locals.clone_ref(py);
            let ray_task_result_handle =
                RayTaskResultHandle::new(py_task_handle, task_locals, self.worker_id.clone());
            let task_result_handle_awaiter = TaskResultHandleAwaiter::new(
                task_id,
                self.worker_id.clone(),
                ray_task_result_handle,
                result_tx,
                cancel_token,
            );
            task_handles.push(task_result_handle_awaiter);
        }

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
