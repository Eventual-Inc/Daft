use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{task::RayTaskResultHandle, RaySwordfishTask};
use crate::scheduling::{
    task::{SwordfishTask, Task, TaskContext, TaskDetails},
    worker::{Worker, WorkerId},
};

type ActiveTaskDetails = HashMap<TaskContext, TaskDetails>;

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

impl RaySwordfishWorker {
    pub fn mark_task_finished(&mut self, task_context: &TaskContext) {
        self.active_task_details.remove(task_context);
    }

    pub fn submit_tasks(
        &mut self,
        tasks: Vec<SwordfishTask>,
        py: Python<'_>,
        task_locals: &pyo3_async_runtimes::TaskLocals,
    ) -> DaftResult<Vec<RayTaskResultHandle>> {
        let mut task_handles = Vec::with_capacity(tasks.len());
        for task in tasks {
            let task_context = task.task_context();
            let task_details = TaskDetails::from(&task);

            let ray_swordfish_task = RaySwordfishTask::new(task);
            let py_task_handle = self.ray_worker_handle.call_method1(
                py,
                pyo3::intern!(py, "submit_task"),
                (ray_swordfish_task,),
            )?;
            let coroutine = py_task_handle.call_method0(py, pyo3::intern!(py, "get_result"))?;

            self.active_task_details.insert(task_context, task_details);

            let task_locals = task_locals.clone_ref(py);
            let ray_task_result_handle = RayTaskResultHandle::new(
                task_context,
                py_task_handle,
                coroutine,
                task_locals,
                self.worker_id.clone(),
            );
            task_handles.push(ray_task_result_handle);
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
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    fn active_num_gpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_gpus())
            .sum()
    }

    fn active_task_details(&self) -> HashMap<TaskContext, TaskDetails> {
        self.active_task_details.clone()
    }
}
