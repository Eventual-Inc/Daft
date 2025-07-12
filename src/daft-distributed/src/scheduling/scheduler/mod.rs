use std::{cmp::Ordering, collections::HashMap};

use super::{
    task::{SchedulingStrategy, Task, TaskDetails},
    worker::{Worker, WorkerId},
};
use crate::{
    pipeline_node::MaterializedOutput, scheduling::task::TaskContext, utils::channel::OneshotSender,
};

mod default;
mod linear;
mod scheduler_actor;

use common_error::DaftResult;
pub(crate) use scheduler_actor::{
    spawn_default_scheduler_actor, SchedulerHandle, SubmittableTask, SubmittedTask,
};
use tokio_util::sync::CancellationToken;

pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<PendingTask<T>>);
    fn schedule_tasks(&mut self) -> Vec<ScheduledTask<T>>;
    fn get_autoscaling_request(&mut self) -> Option<usize>;
    fn num_pending_tasks(&self) -> usize;
}

pub(crate) struct PendingTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
    cancel_token: CancellationToken,
}

impl<T: Task> PendingTask<T> {
    pub fn new(
        task: T,
        result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
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

    pub fn task_context(&self) -> TaskContext {
        self.task.task_context()
    }

    pub fn into_inner(
        self,
    ) -> (
        T,
        OneshotSender<DaftResult<Option<MaterializedOutput>>>,
        CancellationToken,
    ) {
        (self.task, self.result_tx, self.cancel_token)
    }
}

impl<T: Task> std::fmt::Debug for PendingTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SchedulableTask({:?}, {:?})",
            self.task_context(),
            TaskDetails::from(&self.task)
        )
    }
}

impl<T: Task> PartialEq for PendingTask<T> {
    fn eq(&self, other: &Self) -> bool {
        self.task.task_id() == other.task.task_id()
    }
}

impl<T: Task> Eq for PendingTask<T> {}

impl<T: Task> PartialOrd for PendingTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Task> Ord for PendingTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task.priority().cmp(&other.task.priority())
    }
}

pub(super) struct ScheduledTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Option<MaterializedOutput>>>,
    cancel_token: CancellationToken,
    worker_id: WorkerId,
}

impl<T: Task> ScheduledTask<T> {
    pub fn new(task: PendingTask<T>, worker_id: WorkerId) -> Self {
        let (task, result_tx, cancel_token) = task.into_inner();
        Self {
            task,
            result_tx,
            cancel_token,
            worker_id,
        }
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id.clone()
    }

    pub fn task(&self) -> T {
        self.task.clone()
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    pub fn into_inner(
        self,
    ) -> (
        WorkerId,
        T,
        OneshotSender<DaftResult<Option<MaterializedOutput>>>,
        CancellationToken,
    ) {
        (self.worker_id, self.task, self.result_tx, self.cancel_token)
    }
}

impl<T: Task> std::fmt::Debug for ScheduledTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScheduledTask(worker_id = {}, {:?}, {:?})",
            self.worker_id,
            self.task.task_context(),
            TaskDetails::from(&self.task)
        )
    }
}

#[derive(Clone)]
pub(crate) struct WorkerSnapshot {
    worker_id: WorkerId,
    total_num_cpus: f64,
    total_num_gpus: f64,
    active_task_details: HashMap<TaskContext, TaskDetails>,
}

impl WorkerSnapshot {
    pub fn new(
        worker_id: WorkerId,
        total_num_cpus: f64,
        total_num_gpus: f64,
        active_task_details: HashMap<TaskContext, TaskDetails>,
    ) -> Self {
        Self {
            worker_id,
            total_num_cpus,
            total_num_gpus,
            active_task_details,
        }
    }

    pub fn active_num_cpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    pub fn active_num_gpus(&self) -> f64 {
        self.active_task_details
            .values()
            .map(|details| details.num_gpus())
            .sum::<f64>()
    }

    pub fn available_num_cpus(&self) -> f64 {
        self.total_num_cpus - self.active_num_cpus()
    }

    pub fn available_num_gpus(&self) -> f64 {
        self.total_num_gpus - self.active_num_gpus()
    }

    #[allow(dead_code)]
    pub fn total_num_cpus(&self) -> f64 {
        self.total_num_cpus
    }

    #[allow(dead_code)]
    pub fn total_num_gpus(&self) -> f64 {
        self.total_num_gpus
    }

    #[cfg(test)]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    // TODO: Potentially include memory as well, and also be able to overschedule tasks.
    pub fn can_schedule_task(&self, task: &impl Task) -> bool {
        self.available_num_cpus() >= task.resource_request().num_cpus()
    }
}

impl std::fmt::Debug for WorkerSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerSnapshot(worker_id = {}, total_num_cpus = {}, total_num_gpus = {}, active_task_details = {:#?})", self.worker_id, self.total_num_cpus, self.total_num_gpus, self.active_task_details)
    }
}

impl<W: Worker> From<&W> for WorkerSnapshot {
    fn from(worker: &W) -> Self {
        Self::new(
            worker.id().clone(),
            worker.total_num_cpus(),
            worker.total_num_gpus(),
            worker.active_task_details(),
        )
    }
}

#[cfg(test)]
pub(super) mod test_utils {

    use std::collections::HashMap;

    use super::*;
    use crate::scheduling::{
        task::TaskID,
        tests::{MockTask, MockTaskBuilder},
        worker::tests::MockWorker,
    };

    // Helper function to create workers with given configurations
    pub fn setup_workers(configs: &[(WorkerId, usize)]) -> HashMap<WorkerId, MockWorker> {
        configs
            .iter()
            .map(|(id, num_slots)| {
                let worker = MockWorker::new(id.clone(), *num_slots as f64, 0.0);
                (id.clone(), worker)
            })
            .collect::<HashMap<_, _>>()
    }

    // Helper function to setup scheduler with workers
    pub fn setup_scheduler<S: Scheduler<MockTask> + Default>(
        workers: &HashMap<WorkerId, MockWorker>,
    ) -> S {
        let mut scheduler = S::default();
        scheduler.update_worker_state(
            workers
                .values()
                .map(|w| WorkerSnapshot::from(w))
                .collect::<Vec<_>>()
                .as_slice(),
        );
        scheduler
    }

    pub fn create_schedulable_task(mock_task: MockTask) -> PendingTask<MockTask> {
        PendingTask::new(
            mock_task,
            tokio::sync::oneshot::channel().0,
            tokio_util::sync::CancellationToken::new(),
        )
    }

    pub fn create_spread_task(id: Option<TaskID>) -> PendingTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::Spread)
            .with_task_id(id.unwrap_or_default())
            .build();
        create_schedulable_task(task)
    }

    pub fn create_worker_affinity_task(
        worker_id: &WorkerId,
        soft: bool,
        id: Option<TaskID>,
    ) -> PendingTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                worker_id: worker_id.clone(),
                soft,
            })
            .with_task_id(id.unwrap_or_default())
            .build();
        create_schedulable_task(task)
    }
}
