use std::{cmp::Ordering, collections::HashMap};

use super::{
    task::{SchedulingStrategy, Task, TaskDetails, TaskId},
    worker::{Worker, WorkerId},
};
use crate::{pipeline_node::MaterializedOutput, utils::channel::OneshotSender};

mod default;
mod linear;
mod scheduler_actor;

use common_error::DaftResult;
pub(crate) use scheduler_actor::{spawn_default_scheduler_actor, SchedulerHandle, SubmittedTask};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>);
    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>>;
    fn num_pending_tasks(&self) -> usize;
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SchedulableTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
    cancel_token: CancellationToken,
}

#[allow(dead_code)]
impl<T: Task> SchedulableTask<T> {
    pub fn new(
        task: T,
        result_tx: OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
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
    pub fn task_id(&self) -> &TaskId {
        self.task.task_id()
    }

    pub fn into_inner(
        self,
    ) -> (
        T,
        OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
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
#[derive(Debug)]
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
    total_num_cpus: usize,
    active_task_details: HashMap<TaskId, TaskDetails>,
}

#[allow(dead_code)]
impl WorkerSnapshot {
    pub fn new(
        worker_id: WorkerId,
        total_num_cpus: usize,
        active_task_details: HashMap<TaskId, TaskDetails>,
    ) -> Self {
        Self {
            worker_id,
            total_num_cpus,
            active_task_details,
        }
    }

    pub fn active_num_cpus(&self) -> usize {
        self.active_task_details
            .values()
            .map(|details| details.num_cpus())
            .sum()
    }

    pub fn available_num_cpus(&self) -> usize {
        self.total_num_cpus - self.active_num_cpus()
    }

    pub fn total_num_cpus(&self) -> usize {
        self.total_num_cpus
    }
}

impl<W: Worker> From<&W> for WorkerSnapshot {
    fn from(worker: &W) -> Self {
        Self::new(
            worker.id().clone(),
            worker.total_num_cpus(),
            worker.active_task_details(),
        )
    }
}

#[cfg(test)]
pub(super) mod test_utils {

    use std::collections::HashMap;

    use super::*;
    use crate::scheduling::{
        tests::{MockTask, MockTaskBuilder},
        worker::tests::MockWorker,
    };

    // Helper function to create workers with given configurations
    pub fn setup_workers(configs: &[(WorkerId, usize)]) -> HashMap<WorkerId, MockWorker> {
        configs
            .iter()
            .map(|(id, num_slots)| {
                let worker = MockWorker::new(id.clone(), *num_slots);
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

    pub fn create_schedulable_task(mock_task: MockTask) -> SchedulableTask<MockTask> {
        SchedulableTask::new(
            mock_task,
            tokio::sync::oneshot::channel().0,
            tokio_util::sync::CancellationToken::new(),
        )
    }

    pub fn create_spread_task() -> SchedulableTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::Spread)
            .build();
        create_schedulable_task(task)
    }

    pub fn create_worker_affinity_task(
        worker_id: &WorkerId,
        soft: bool,
    ) -> SchedulableTask<MockTask> {
        let task = MockTaskBuilder::default()
            .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                worker_id: worker_id.clone(),
                soft,
            })
            .build();
        create_schedulable_task(task)
    }
}
