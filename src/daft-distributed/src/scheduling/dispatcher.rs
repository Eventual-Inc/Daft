use std::{
    collections::HashMap,
    future::Future,
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
        mut dispatcher: Self,
        mut task_rx: Receiver<Vec<SchedulableTask<T>>>,
    ) -> DaftResult<()> {
        let mut running_tasks = JoinSet::new();
        let mut running_tasks_by_id = HashMap::new();
        let mut pending_tasks: Vec<SchedulableTask<T>> =
            Vec::with_capacity(dispatcher.worker_manager.total_available_cpus());

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
                            None
                        },
                        // If the task is finished, return the result and the result_tx
                        result = task_handle.get_result() => {
                            Some((result, task.result_tx))
                        }
                    }
                };
                let joinset_id = running_tasks.spawn(task_future).id();
                running_tasks_by_id.insert(joinset_id, (worker_id, task_id));
            }

            // 4. Wait for tasks to finish or receive new tasks
            tokio::select! {
                biased;
                Some(finished_task) = running_tasks.join_next_with_id() => {
                    match finished_task {
                        Ok((joinset_id, task_result)) => {
                            let (worker_id, task_id) = running_tasks_by_id.remove(&joinset_id).unwrap();
                            dispatcher.worker_manager.mark_task_finished(task_id, worker_id);
                            match task_result {
                                Some((result, result_tx)) => {
                                    let _ = result_tx.send(result);
                                }
                                None => {}
                            }
                        }
                        Err(e) => {
                            let joinset_id = e.id();
                            let (worker_id, task_id) = running_tasks_by_id.remove(&joinset_id).unwrap();
                            dispatcher.worker_manager.mark_task_finished(task_id, worker_id);
                        }
                    }
                }
                Some(next_tasks) = task_rx.recv() => {
                    pending_tasks.extend(next_tasks);
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

    pub fn task_id(&self) -> &str {
        self.task.task_id()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TaskDispatcherHandle<T: Task> {
    task_dispatcher_sender: Sender<Vec<SchedulableTask<T>>>,
}

impl<T: Task> TaskDispatcherHandle<T> {
    fn new(task_dispatcher_sender: Sender<Vec<SchedulableTask<T>>>) -> Self {
        Self {
            task_dispatcher_sender,
        }
    }

    fn prepare_tasks_for_submission(
        &self,
        tasks: Vec<T>,
    ) -> (Vec<SchedulableTask<T>>, Vec<SubmittedTask>) {
        let mut schedulable_tasks = Vec::with_capacity(tasks.len());
        let mut submitted_tasks = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (result_tx, result_rx) = create_oneshot_channel();
            let cancel_token = CancellationToken::new();
            let submitted_task = SubmittedTask::new(
                task.task_id().to_string(),
                result_rx,
                Some(cancel_token.clone()),
            );
            let schedulable_task = SchedulableTask::new(task, result_tx, cancel_token);
            schedulable_tasks.push(schedulable_task);
            submitted_tasks.push(submitted_task);
        }
        (schedulable_tasks, submitted_tasks)
    }

    pub async fn submit_task(&self, task: T) -> DaftResult<SubmittedTask> {
        let (schedulable_tasks, mut submitted_tasks) =
            self.prepare_tasks_for_submission(vec![task]);
        let _ = self.task_dispatcher_sender.send(schedulable_tasks).await;
        Ok(submitted_tasks.pop().unwrap())
    }

    pub async fn submit_many_tasks(&self, tasks: Vec<T>) -> DaftResult<Vec<SubmittedTask>> {
        let (schedulable_tasks, submitted_tasks) = self.prepare_tasks_for_submission(tasks);
        let _ = self.task_dispatcher_sender.send(schedulable_tasks).await;
        Ok(submitted_tasks)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittedTask {
    task_id: TaskId,
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
