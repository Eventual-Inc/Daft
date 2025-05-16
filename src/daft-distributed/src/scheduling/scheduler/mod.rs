use std::collections::HashSet;

use super::{
    task::{Task, TaskId},
    worker::{Worker, WorkerId},
};

mod default;
mod linear;
mod scheduler_actor;

use scheduler_actor::SchedulableTask;
pub(crate) use scheduler_actor::{SchedulerActor, SchedulerHandle};

#[allow(dead_code)]
pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>);
    fn get_scheduled_tasks(&mut self) -> Vec<(WorkerId, SchedulableTask<T>)>;
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) struct WorkerSnapshot {
    worker_id: WorkerId,
    num_cpus: usize,
    active_task_ids: HashSet<TaskId>,
}

#[allow(dead_code)]
impl WorkerSnapshot {
    fn new(worker: &impl Worker) -> Self {
        Self {
            worker_id: worker.id().clone(),
            active_task_ids: worker.active_task_ids(),
            num_cpus: worker.num_cpus(),
        }
    }
}
