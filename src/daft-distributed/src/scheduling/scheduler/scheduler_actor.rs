use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use super::{default::DefaultScheduler, linear::LinearScheduler, SchedulableTask, Scheduler};
use crate::{
    scheduling::{
        dispatcher::{DispatcherActor, DispatcherHandle},
        task::{Task, TaskId},
        worker::{Worker, WorkerManager},
    },
    utils::{
        channel::{create_channel, create_oneshot_channel, OneshotReceiver, Receiver, Sender},
        joinset::JoinSet,
    },
};

#[allow(dead_code)]
struct SchedulerActor<W: Worker, S: Scheduler<W::Task>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    scheduler: S,
}

impl<W: Worker> SchedulerActor<W, DefaultScheduler<W::Task>> {
    fn default_scheduler(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self {
            worker_manager,
            scheduler: DefaultScheduler::default(),
        }
    }
}

#[allow(dead_code)]
impl<W: Worker> SchedulerActor<W, LinearScheduler<W::Task>> {
    fn linear_scheduler(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self {
            worker_manager,
            scheduler: LinearScheduler::default(),
        }
    }
}

impl<W, S> SchedulerActor<W, S>
where
    W: Worker,
    S: Scheduler<W::Task> + Send + 'static,
{
    fn spawn_scheduler_actor(
        scheduler: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> SchedulerHandle<W::Task> {
        // Spawn a dispatcher actor to handle task dispatch and await task results
        let dispatcher_actor = DispatcherActor::new(scheduler.worker_manager);
        let dispatcher_handle = DispatcherActor::spawn_dispatcher_actor(dispatcher_actor, joinset);

        // Spawn the scheduler actor to schedule tasks and dispatch them to the dispatcher
        let (scheduler_sender, scheduler_receiver) = create_channel(1);
        joinset.spawn(Self::run_scheduler_loop(
            scheduler.scheduler,
            scheduler_receiver,
            dispatcher_handle,
        ));
        SchedulerHandle::new(scheduler_sender)
    }

    async fn run_scheduler_loop(
        _scheduler: S,
        _task_rx: Receiver<SchedulableTask<W::Task>>,
        _dispatcher_handle: DispatcherHandle<W::Task>,
    ) -> DaftResult<()> {
        todo!("FLOTILLA_MS1: Implement run scheduler loop");
    }
}

pub(crate) fn spawn_default_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
) -> SchedulerHandle<W::Task> {
    let scheduler = SchedulerActor::default_scheduler(worker_manager);
    SchedulerActor::spawn_scheduler_actor(scheduler, joinset)
}

#[allow(dead_code)]
pub(crate) fn spawn_linear_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
) -> SchedulerHandle<W::Task> {
    let scheduler = SchedulerActor::linear_scheduler(worker_manager);
    SchedulerActor::spawn_scheduler_actor(scheduler, joinset)
}

#[derive(Clone, Debug)]
pub(crate) struct SchedulerHandle<T: Task> {
    scheduler_sender: Sender<SchedulableTask<T>>,
}

impl<T: Task> SchedulerHandle<T> {
    fn new(scheduler_sender: Sender<SchedulableTask<T>>) -> Self {
        Self { scheduler_sender }
    }

    fn prepare_task_for_submission(&self, task: T) -> (SchedulableTask<T>, SubmittedTask) {
        let (result_tx, result_rx) = create_oneshot_channel();
        let cancel_token = CancellationToken::new();

        let task_id = task.task_id().clone();
        let schedulable_task = SchedulableTask::new(task, result_tx, cancel_token.clone());
        let submitted_task = SubmittedTask::new(task_id, result_rx, Some(cancel_token));

        (schedulable_task, submitted_task)
    }

    #[allow(dead_code)]
    pub async fn submit_task(&self, task: T) -> DaftResult<SubmittedTask> {
        let (schedulable_task, submitted_task) = self.prepare_task_for_submission(task);
        self.scheduler_sender
            .send(schedulable_task)
            .await
            .map_err(|_| {
                DaftError::InternalError("Failed to send task to scheduler".to_string())
            })?;
        Ok(submitted_task)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittedTask {
    _task_id: TaskId,
    result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
    cancel_token: Option<CancellationToken>,
    finished: bool,
}

impl SubmittedTask {
    fn new(
        task_id: TaskId,
        result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            _task_id: task_id,
            result_rx,
            cancel_token,
            finished: false,
        }
    }

    #[allow(dead_code)]
    pub fn id(&self) -> &TaskId {
        &self._task_id
    }
}

impl Future for SubmittedTask {
    type Output = Option<DaftResult<PartitionRef>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result_rx.poll_unpin(cx) {
            Poll::Ready(Ok(result)) => {
                self.finished = true;
                Poll::Ready(Some(result))
            }
            Poll::Ready(Err(_)) => {
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for SubmittedTask {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(cancel_token) = self.cancel_token.take() {
            cancel_token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_scheduler_actor_basic_task() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor submit task")
    }

    #[test]
    #[ignore]
    fn test_scheduler_actor_multiple_tasks() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor multiple tasks")
    }

    #[test]
    #[ignore]
    fn test_scheduler_actor_multiple_concurrent_tasks() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor multiple concurrent tasks")
    }

    #[test]
    #[ignore]
    fn test_scheduler_actor_cancelled_task() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor cancelled tasks")
    }

    #[test]
    #[ignore]
    fn test_scheduler_actor_many_cancelled_tasks() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor many cancelled tasks")
    }

    #[test]
    #[ignore]
    fn test_scheduler_actor_error_from_task() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor error from task")
    }

    #[test]
    #[ignore]
    fn test_scheduler_actor_panic_from_task() {
        todo!("FLOTILLA_MS1: Implement test for scheduler actor panic from task")
    }
}
