use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, RaySwordfishTask};
use crate::scheduling::{
    scheduler::SchedulableTask,
    task::{SwordfishTask, Task, TaskDetails, TaskID, TaskResultHandleAwaiter},
    worker::{Worker, WorkerId},
};

type ActiveTaskDetails = Arc<Mutex<HashMap<TaskID, TaskDetails>>>;

#[pyclass(module = "daft.daft", name = "RaySwordfishWorker")]
#[derive(Debug, Clone)]
pub(crate) struct RaySwordfishWorker {
    worker_id: WorkerId,
    ray_worker_handle: Arc<PyObject>,
    num_cpus: f64,
    #[allow(dead_code)]
    total_memory_bytes: usize,
    num_gpus: f64,
    active_task_details: ActiveTaskDetails,
}

#[pymethods]
impl RaySwordfishWorker {
    #[new]
    pub fn new(
        worker_id: String,
        ray_worker_handle: PyObject,
        num_cpus: f64,
        num_gpus: f64,
        total_memory_bytes: usize,
    ) -> Self {
        Self {
            worker_id: Arc::from(worker_id),
            ray_worker_handle: Arc::new(ray_worker_handle),
            num_cpus,
            num_gpus,
            total_memory_bytes,
            active_task_details: Default::default(),
        }
    }
}

#[allow(dead_code)]
impl RaySwordfishWorker {
    pub fn mark_task_finished(&self, task_id: &TaskID) {
        self.active_task_details
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
            let task_id: Arc<str> = Arc::from(task.task_id().to_string());
            let task_details = TaskDetails::from(&task);

            let ray_swordfish_task = RaySwordfishTask::new(task);
            let py_task_handle = self.ray_worker_handle.call_method1(
                py,
                pyo3::intern!(py, "submit_task"),
                (ray_swordfish_task,),
            )?;
            let coroutine = py_task_handle.call_method0(py, pyo3::intern!(py, "get_result"))?;

            self.active_task_details
                .lock()
                .expect("Active task details should be present")
                .insert(Arc::from(task_id.to_string()), task_details);

            let task_locals = task_locals.clone_ref(py);
            let ray_task_result_handle = RayTaskResultHandle::new(
                py_task_handle,
                coroutine,
                task_locals,
                self.worker_id.clone(),
            );
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
        self.ray_worker_handle
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

    fn total_num_cpus(&self) -> f64 {
        self.num_cpus
    }

    fn total_num_gpus(&self) -> f64 {
        self.num_gpus
    }

    fn active_num_cpus(&self) -> f64 {
        self.active_task_details
            .lock()
            .expect("Active task details should be present")
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    fn active_num_gpus(&self) -> f64 {
        self.active_task_details
            .lock()
            .expect("Active task details should be present")
            .values()
            .map(|details| details.num_gpus())
            .sum()
    }

    fn active_task_details(&self) -> HashMap<TaskID, TaskDetails> {
        self.active_task_details
            .lock()
            .expect("Active task details should be present")
            .clone()
    }
}
