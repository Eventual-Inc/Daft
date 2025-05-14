use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use super::{
    scheduler::{DefaultScheduler, Scheduler},
    task::{SchedulingStrategy, Task, TaskId},
    worker::Worker,
};
use crate::{
    scheduling::worker::WorkerManager,
    utils::{
        channel::{
            create_channel, create_oneshot_channel, OneshotReceiver, OneshotSender, Receiver,
            Sender,
        },
        joinset::JoinSet,
    },
};
// The task dispatcher is responsible for dispatching tasks to workers.
#[allow(dead_code)]
pub(crate) struct TaskDispatcher<T: Task, W: Worker> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    scheduler: Box<dyn Scheduler<T, W>>,
}

impl<T: Task, W: Worker> TaskDispatcher<T, W> {
    const MAX_TASKS_IN_CHANNEL: usize = 512;

    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        let scheduler = Box::new(DefaultScheduler::new());
        Self {
            worker_manager,
            scheduler,
        }
    }

    #[allow(dead_code)]
    pub fn new_with_scheduler(
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        scheduler: Box<dyn Scheduler<T, W>>,
    ) -> Self {
        Self {
            worker_manager,
            scheduler,
        }
    }

    pub fn spawn_task_dispatcher(
        task_dispatcher: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> TaskDispatcherHandle<T> {
        let (task_dispatcher_sender, task_dispatcher_receiver) =
            create_channel(Self::MAX_TASKS_IN_CHANNEL);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        TaskDispatcherHandle::new(task_dispatcher_sender)
    }

    async fn run_dispatch_loop(
        _dispatcher: Self,
        _task_rx: Receiver<SchedulableTask<T>>,
    ) -> DaftResult<()> {
        todo!("FLOTILLA_MS1: Implement run dispatch loop");
    }
}

#[allow(dead_code)]
pub(crate) struct SchedulableTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Vec<PartitionRef>>>,
    cancel_token: CancellationToken,
}

impl<T: Task> SchedulableTask<T> {
    pub fn new(
        task: T,
        result_tx: OneshotSender<DaftResult<Vec<PartitionRef>>>,
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
    pub fn task_id(&self) -> &str {
        self.task.task_id()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TaskDispatcherHandle<T: Task> {
    task_dispatcher_sender: Sender<SchedulableTask<T>>,
}

impl<T: Task> TaskDispatcherHandle<T> {
    fn new(task_dispatcher_sender: Sender<SchedulableTask<T>>) -> Self {
        Self {
            task_dispatcher_sender,
        }
    }

    fn prepare_task_for_submission(&self, task: T) -> (SchedulableTask<T>, SubmittedTask) {
        let (result_tx, result_rx) = create_oneshot_channel();
        let cancel_token = CancellationToken::new();

        let task_id = task.task_id().to_string();
        let schedulable_task = SchedulableTask::new(task, result_tx, cancel_token.clone());
        let submitted_task = SubmittedTask::new(task_id, result_rx, Some(cancel_token));

        (schedulable_task, submitted_task)
    }

    #[allow(dead_code)]
    pub async fn submit_task(&self, task: T) -> DaftResult<SubmittedTask> {
        let (schedulable_task, submitted_task) = self.prepare_task_for_submission(task);
        let _ = self.task_dispatcher_sender.send(schedulable_task).await;
        Ok(submitted_task)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittedTask {
    _task_id: TaskId,
    result_rx: OneshotReceiver<DaftResult<Vec<PartitionRef>>>,
    cancel_token: Option<CancellationToken>,
    finished: bool,
}

impl SubmittedTask {
    fn new(
        task_id: TaskId,
        result_rx: OneshotReceiver<DaftResult<Vec<PartitionRef>>>,
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
    pub fn id(&self) -> &str {
        &self._task_id
    }
}

impl Future for SubmittedTask {
    type Output = Option<DaftResult<Vec<PartitionRef>>>;

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
