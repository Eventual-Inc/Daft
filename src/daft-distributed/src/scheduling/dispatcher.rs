use std::{
    collections::HashMap,
    future::Future,
    os::fd::OwnedFd,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use super::{
    scheduler::{DefaultScheduler, Scheduler},
    task::{SchedulingStrategy, Task, TaskId},
    worker::Worker,
};
use crate::{
    pipeline_node::MaterializedOutput, scheduling::worker::WorkerManager, utils::{
        channel::{
            create_channel, create_oneshot_channel, OneshotReceiver, OneshotSender, Receiver,
            Sender,
        },
        joinset::JoinSet,
    }
};
// The task dispatcher is responsible for dispatching tasks to workers.
pub(crate) struct TaskDispatcher<T: Task, W: Worker> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    scheduler: Box<dyn Scheduler<T, W>>,
}

impl<T: Task, W: Worker> TaskDispatcher<T, W> {
    const MAX_EXTRA_TASKS: usize = 180;

    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        let scheduler = Box::new(DefaultScheduler::new());
        Self {
            worker_manager,
            scheduler,
        }
    }

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
    ) -> TaskDispatcherHandleRef<T> {
        let (task_dispatcher_sender, task_dispatcher_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        Arc::new(TaskDispatcherHandle::new(task_dispatcher_sender))
    }

    async fn run_dispatch_loop(
        mut dispatcher: Self,
        mut task_rx: Receiver<SchedulableTask<T>>,
    ) -> DaftResult<()> {
        let mut running_tasks = JoinSet::new();
        let mut running_tasks_by_id = HashMap::new();
        let mut pending_tasks: Vec<SchedulableTask<T>> = Vec::with_capacity(
            dispatcher.worker_manager.total_available_cpus() + Self::MAX_EXTRA_TASKS,
        );

        loop {
            // 1. Update the scheduler with the current state of the workers
            let workers = dispatcher.worker_manager.workers();
            dispatcher.scheduler.update_state(workers);

            // 2. Get tasks to schedule
            let schedule_result = dispatcher
                .scheduler
                .schedule_tasks(std::mem::take(&mut pending_tasks));
            let schedulable_task_and_worker_ids = schedule_result.scheduled_tasks;
            pending_tasks = schedule_result.unscheduled_tasks;

            // 3. Schedule tasks
            for (worker_id, task) in schedulable_task_and_worker_ids.into_iter() {
                let task_id = task.task_id().to_string();
                let mut task_handle = dispatcher
                    .worker_manager
                    .submit_task_to_worker(Box::new(task.task), worker_id.clone());

                let task_future = async move {
                    tokio::select! {
                        biased;
                        // If the task was cancelled, return None
                        () = task.cancel_token.cancelled() => {
                            if let Err(e) = task_handle.cancel_callback() {
                                eprintln!("Error cancelling task: {:?}", e);
                            }
                        },
                        // If the task is finished, return the result and the result_tx
                        result = task_handle.get_result() => {
                            // Ignore the send error here because the receiver may be dropped, i.e. cancelled
                            let _ = task.result_tx.send(result);
                        }
                    }
                };
                let joinset_id = running_tasks.spawn(task_future);
                running_tasks_by_id.insert(joinset_id, (worker_id, task_id));
            }

            // 4. Wait for tasks to finish or receive new tasks
            let max_tasks_to_wait_for =
                dispatcher.worker_manager.total_available_cpus() + Self::MAX_EXTRA_TASKS;
            let current_pending_tasks = pending_tasks.len();
            tokio::select! {
                biased;
                Some((joinset_id, finished_task)) = running_tasks.join_next_with_id() => {
                    let (worker_id, task_id) = running_tasks_by_id.remove(&joinset_id).unwrap();
                    dispatcher.worker_manager.mark_task_finished(task_id, worker_id);
                    // If there is an error here it means there was a panic during the 'get_result' call, which we should propagate
                    if let Err(e) = finished_task {
                        return Err(e);
                    }
                }
                Some(next_task) = task_rx.recv(), if current_pending_tasks < max_tasks_to_wait_for => {
                    pending_tasks.push(next_task);
                }
                else => {
                    assert!(running_tasks.is_empty());
                    assert!(task_rx.is_closed());
                    break;
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct SchedulableTask<T: Task> {
    task: T,
    result_tx: OneshotSender<DaftResult<Vec<MaterializedOutput>>>,
    cancel_token: CancellationToken,
}

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

    pub fn task_id(&self) -> &str {
        self.task.task_id()
    }
}

pub(crate) type TaskDispatcherHandleRef<T> = Arc<TaskDispatcherHandle<T>>;

#[derive(Debug)]
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
        let submitted_task = SubmittedTask::new(
            task.task_id().to_string(),
            result_rx,
            Some(cancel_token.clone()),
        );
        let schedulable_task = SchedulableTask::new(task, result_tx, cancel_token);
        (schedulable_task, submitted_task)
    }

    pub async fn submit_task(&self, task: T) -> DaftResult<SubmittedTask> {
        let (schedulable_task, submitted_task) = self.prepare_task_for_submission(task);
        self.task_dispatcher_sender
            .send(schedulable_task)
            .await
            .map_err(|_| {
                DaftError::InternalError(format!(
                    "Failed to send task to task dispatcher: task dispatcher has been dropped",
                ))
            })?;
        Ok(submitted_task)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittedTask {
    task_id: TaskId,
    result_rx: OneshotReceiver<DaftResult<Vec<MaterializedOutput>>>,
    cancel_token: Option<CancellationToken>,
    finished: bool,
}

impl SubmittedTask {
    fn new(
        task_id: TaskId,
        result_rx: OneshotReceiver<DaftResult<Vec<MaterializedOutput>>>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            task_id,
            result_rx,
            cancel_token,
            finished: false,
        }
    }

    pub fn id(&self) -> &str {
        &self.task_id
    }
}

impl Future for SubmittedTask {
    type Output = Option<DaftResult<Vec<MaterializedOutput>>>;

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
