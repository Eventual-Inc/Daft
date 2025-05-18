use std::{collections::HashSet, future::Future, sync::Arc};

use super::{
    task::{Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager},
};

mod default;
mod linear;
mod scheduler_actor;

use scheduler_actor::SchedulableTask;
pub(crate) use scheduler_actor::{SchedulerActor, SchedulerHandle, SubmittedTask};

pub(super) struct ScheduledTask<T: Task> {
    pub task: SchedulableTask<T>,
    pub worker_id: WorkerId,
}

#[allow(dead_code)]
impl<T: Task> ScheduledTask<T> {
    pub fn new(task: SchedulableTask<T>, worker_id: WorkerId) -> Self {
        Self { task, worker_id }
    }

    pub fn submit_task<W: Worker>(
        self,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
    ) -> impl Future<Output = ()> {
        let (task, result_tx, cancel_token) = self.task.into_inner();
        let mut task_handle = worker_manager.submit_task_to_worker(Box::new(task), self.worker_id);

        async move {
            tokio::select! {
                biased;
                () = cancel_token.cancelled() => {},
                result = task_handle.get_result() => {
                    // Ignore the send error here because the receiver may be dropped, i.e. cancelled
                    tracing::debug!("Unable to send result of task, receiver was dropped");
                    let _ = result_tx.send(result);
                }
            }
        }
    }
}

#[allow(dead_code)]
pub(super) trait Scheduler<T: Task>: Send + Sync {
    fn update_worker_state(&mut self, worker_snapshots: &[WorkerSnapshot]);
    fn enqueue_tasks(&mut self, tasks: Vec<SchedulableTask<T>>);
    fn get_schedulable_tasks(&mut self) -> Vec<ScheduledTask<T>>;
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
