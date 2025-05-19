use std::{cmp::Ordering, collections::HashSet};

use super::{
    task::{SchedulingStrategy, Task, TaskId},
    worker::{Worker, WorkerId},
};
use crate::utils::channel::OneshotSender;

mod default;
mod linear;
mod scheduler_actor;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
pub(crate) use scheduler_actor::{spawn_default_scheduler_actor, SchedulerHandle, SubmittedTask};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>);
    fn num_pending_tasks(&self) -> usize;
    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>>;
}

#[allow(dead_code)]
pub(crate) struct SchedulableTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<PartitionRef>>,
    cancel_token: CancellationToken,
}

#[allow(dead_code)]
impl<T: Task> SchedulableTask<T> {
    pub fn new(
        task: T,
        result_tx: OneshotSender<DaftResult<PartitionRef>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task,
            result_tx,
            cancel_token,
        }
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        self.task.strategy()
    }

    #[allow(dead_code)]
    pub fn priority(&self) -> u32 {
        self.task.priority()
    }

    #[allow(dead_code)]
    pub fn task_id(&self) -> &str {
        self.task.task_id()
    }

    pub fn into_inner(
        self,
    ) -> (
        T,
        OneshotSender<DaftResult<PartitionRef>>,
        CancellationToken,
    ) {
        (self.task, self.result_tx, self.cancel_token)
    }
}

impl<T: Task> PartialEq for SchedulableTask<T> {
    fn eq(&self, other: &Self) -> bool {
        self.task.task_id() == other.task.task_id()
    }
}

impl<T: Task> Eq for SchedulableTask<T> {}

impl<T: Task> PartialOrd for SchedulableTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Task> Ord for SchedulableTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task.priority().cmp(&other.task.priority())
    }
}

#[allow(dead_code)]
pub(super) struct ScheduledTask<T: Task> {
    task: SchedulableTask<T>,
    worker_id: WorkerId,
}

#[allow(dead_code)]
impl<T: Task> ScheduledTask<T> {
    pub fn new(task: SchedulableTask<T>, worker_id: WorkerId) -> Self {
        Self { task, worker_id }
    }

    pub fn into_inner(self) -> (WorkerId, SchedulableTask<T>) {
        (self.worker_id, self.task)
    }
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
    pub fn new(worker: &impl Worker) -> Self {
        Self {
            worker_id: worker.id().clone(),
            active_task_ids: worker.active_task_ids(),
            num_cpus: worker.num_cpus(),
        }
    }
}
