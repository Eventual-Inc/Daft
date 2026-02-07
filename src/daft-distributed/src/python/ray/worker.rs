use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use common_error::DaftResult;
use pyo3::prelude::*;

use super::{RaySwordfishTask, task::RayTaskResultHandle};
use crate::scheduling::{
    task::{SwordfishTask, Task, TaskContext, TaskDetails},
    worker::{Worker, WorkerId},
};

type ActiveTaskDetails = HashMap<TaskContext, TaskDetails>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ActorState {
    Starting,
    Ready,
    Busy,
    Idle,
    Releasing,
    Released,
}

#[pyclass(module = "daft.daft", name = "RaySwordfishWorker")]
#[derive(Debug, Clone)]
pub(crate) struct RaySwordfishWorker {
    worker_id: WorkerId,
    ray_worker_handle: Arc<Py<PyAny>>,
    num_cpus: f64,
    total_memory_bytes: usize,
    num_gpus: f64,
    active_task_details: ActiveTaskDetails,
    last_task_finished_at: Instant,
    state: ActorState,
}

#[pymethods]
impl RaySwordfishWorker {
    #[new]
    pub fn new(
        worker_id: String,
        ray_worker_handle: pyo3::Py<pyo3::PyAny>,
        num_cpus: f64,
        num_gpus: f64,
        total_memory_bytes: usize,
    ) -> Self {
        let mut w = Self {
            worker_id: Arc::from(worker_id),
            ray_worker_handle: Arc::new(ray_worker_handle),
            num_cpus,
            num_gpus,
            total_memory_bytes,
            active_task_details: Default::default(),
            last_task_finished_at: Instant::now(),
            state: ActorState::Starting,
        };
        w.set_state(ActorState::Ready);
        w
    }
}

impl RaySwordfishWorker {
    pub fn total_memory_bytes(&self) -> usize {
        self.total_memory_bytes
    }

    pub fn set_state(&mut self, state: ActorState) {
        self.state = state;
    }

    pub fn mark_task_finished(&mut self, task_context: &TaskContext) {
        self.active_task_details.remove(task_context);
        let now = Instant::now();
        self.last_task_finished_at = now;
        if self.active_task_details.is_empty() {
            self.set_state(ActorState::Idle);
        }
    }

    pub fn submit_tasks(
        &mut self,
        tasks: Vec<SwordfishTask>,
        py: Python<'_>,
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

            self.active_task_details
                .insert(task_context.clone(), task_details);
            if self.active_task_details.len() == 1 {
                self.set_state(ActorState::Busy);
            }

            let ray_task_result_handle = RayTaskResultHandle::new(
                task_context,
                py_task_handle,
                coroutine,
                self.worker_id.clone(),
            );
            task_handles.push(ray_task_result_handle);
        }

        Ok(task_handles)
    }

    pub fn is_idle(&self) -> bool {
        self.active_task_details.is_empty()
    }

    pub fn idle_duration(&self, now: Instant) -> Duration {
        if self.is_idle() {
            now.saturating_duration_since(self.last_task_finished_at)
        } else {
            Duration::from_secs(0)
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&self, py: Python<'_>) {
        self.ray_worker_handle
            .call_method0(py, pyo3::intern!(py, "shutdown"))
            .expect("Failed to shutdown RaySwordfishWorker");
    }

    pub fn release(&mut self, py: Python<'_>) {
        let inflight = self.active_task_details.len();
        if inflight > 0 {
            tracing::warn!(
                target: "ray_swordfish_worker",
                worker_id = %self.worker_id,
                inflight_tasks = inflight,
                "Cannot release worker because it has active tasks."
            );
            return;
        }
        self.set_state(ActorState::Releasing);
        self.shutdown(py);
        self.set_state(ActorState::Released);
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use pyo3::prelude::*;

    use super::*;
    use crate::scheduling::{task::TaskDetails, tests::MockTaskBuilder};

    #[test]
    fn test_new_initial_state_ready() {
        Python::with_gil(|py| {
            let handle: Py<PyAny> = py.None();
            let worker = RaySwordfishWorker::new("worker-1".to_string(), handle, 1.0, 0.0, 1024);
            assert!(matches!(worker.state, ActorState::Ready));
        });
    }

    #[test]
    fn test_mark_task_finished_moves_to_idle_and_updates_timestamp() {
        Python::with_gil(|py| {
            let handle: Py<PyAny> = py.None();
            let mut worker =
                RaySwordfishWorker::new("worker-1".to_string(), handle, 1.0, 0.0, 1024);

            let task = MockTaskBuilder::default().with_task_id(1).build();
            let ctx = task.task_context();
            let details = TaskDetails::from(&task);

            worker.active_task_details.insert(ctx.clone(), details);

            worker.set_state(ActorState::Busy);

            let before = worker.last_task_finished_at;
            worker.mark_task_finished(&ctx);
            let after = worker.last_task_finished_at;

            assert!(worker.active_task_details.is_empty());
            assert!(matches!(worker.state, ActorState::Idle));
            assert!(after >= before);
        });
    }

    #[test]
    fn test_idle_duration_zero_when_busy_and_positive_when_idle() {
        Python::with_gil(|py| {
            let handle: Py<PyAny> = py.None();
            let mut worker =
                RaySwordfishWorker::new("worker-1".to_string(), handle, 1.0, 0.0, 1024);

            // Busy state should report zero idle duration.
            worker.set_state(ActorState::Busy);
            worker.last_task_finished_at = Instant::now();
            let now = Instant::now();
            let busy_idle = worker.idle_duration(now);
            assert_eq!(busy_idle.as_secs(), 0);

            // Idle state should report non-zero when last_task_finished_at in the past.
            worker.set_state(ActorState::Idle);
            worker.last_task_finished_at = Instant::now() - Duration::from_secs(5);
            let now2 = Instant::now();
            let idle_idle = worker.idle_duration(now2);
            assert!(idle_idle.as_secs() >= 5);
        });
    }

    #[test]
    fn test_release_does_not_shutdown_when_inflight_tasks() {
        Python::with_gil(|py| {
            let handle: Py<PyAny> = py.None();
            let mut worker =
                RaySwordfishWorker::new("worker-1".to_string(), handle, 1.0, 0.0, 1024);

            let task = MockTaskBuilder::default().with_task_id(1).build();
            let ctx = task.task_context();
            let details = TaskDetails::from(&task);

            worker.active_task_details.insert(ctx.clone(), details);
            worker.set_state(ActorState::Busy);

            worker.release(py);

            assert!(matches!(worker.state, ActorState::Busy));
            assert_eq!(worker.active_task_details.len(), 1);
        });
    }
}
