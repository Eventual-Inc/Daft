use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use super::task::SchedulingStrategy;
use crate::{
    channel::{
        create_channel, create_oneshot_channel, OneshotReceiver, OneshotSender, Receiver, Sender,
    },
    runtime::JoinSet,
    scheduling::{task::SwordfishTask, worker::WorkerManager},
};
// The task dispatcher is responsible for dispatching tasks to workers.
#[allow(dead_code)]
pub(crate) struct TaskDispatcher {
    worker_manager: Box<dyn WorkerManager>,
}

impl TaskDispatcher {
    const MAX_TASKS_IN_CHANNEL: usize = 100;
    const MAX_TASKS_IN_BUFFER: usize = 100;

    pub fn new(worker_manager: Box<dyn WorkerManager>) -> Self {
        Self { worker_manager }
    }

    pub fn spawn_task_dispatcher(
        task_dispatcher: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> TaskDispatcherHandle {
        let (task_dispatcher_sender, task_dispatcher_receiver) =
            create_channel(Self::MAX_TASKS_IN_CHANNEL);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        TaskDispatcherHandle::new(task_dispatcher_sender)
    }

    // Determine which tasks can be scheduled on which workers
    // Returns a list of (task_idx, worker_id) tuples
    fn get_schedulable_task_and_worker_ids(
        queued_tasks: &[DispatchableTask],
        worker_manager: &dyn WorkerManager,
    ) -> Vec<(usize, String)> {
        // Get worker resources and idle workers
        let workers = worker_manager.get_worker_slots();

        let mut schedulable_task_and_worker_ids = Vec::new();

        for (i, task) in queued_tasks.iter().enumerate() {
            match task.task.strategy() {
                SchedulingStrategy::Spread => {
                    // Find the worker with the most available slots
                    let mut max_slots = 0;
                    let mut max_worker_id = String::new();
                    for (worker_id, worker_slots) in &workers {
                        if *worker_slots > max_slots {
                            max_slots = *worker_slots;
                            max_worker_id.clone_from(worker_id);
                        }
                    }
                    schedulable_task_and_worker_ids.push((i, max_worker_id));
                }
                SchedulingStrategy::NodeAffinity { node_id, soft } => {
                    match soft {
                        true => {
                            // Find the worker with the node id, if it has available slots then schedule the task
                            // Otherwise, find another worker with available slots
                            let slots = workers.get(node_id).expect("Worker not found");
                            if *slots > 0 {
                                schedulable_task_and_worker_ids.push((i, node_id.clone()));
                            } else {
                                // Find any worker with available slots
                                for (worker_id, worker_slots) in &workers {
                                    if *worker_slots > 0 {
                                        schedulable_task_and_worker_ids
                                            .push((i, worker_id.clone()));
                                    }
                                }
                            }
                        }
                        false => {
                            // Find the worker with the node id, if it has available slots then schedule the task
                            // Otherwise, skip the task
                            let slots = workers.get(node_id).expect("Worker not found");
                            if *slots > 0 {
                                schedulable_task_and_worker_ids.push((i, node_id.clone()));
                            }
                        }
                    }
                }
            }
        }

        schedulable_task_and_worker_ids
    }

    async fn run_dispatch_loop(
        dispatcher: Self,
        mut task_rx: Receiver<DispatchableTask>,
    ) -> DaftResult<()> {
        let mut pending_tasks = JoinSet::new();
        let mut queued_tasks: Vec<DispatchableTask> = Vec::with_capacity(Self::MAX_TASKS_IN_BUFFER);

        loop {
            // Get schedulable tasks
            let schedulable_task_and_worker_ids = Self::get_schedulable_task_and_worker_ids(
                &queued_tasks,
                &*dispatcher.worker_manager,
            );

            // Schedule the tasks we found workers for (in reverse order to maintain indices, because we do swap_remove)
            for (task_idx, worker_id) in schedulable_task_and_worker_ids.into_iter().rev() {
                let task = queued_tasks.swap_remove(task_idx);
                let mut task_handle = dispatcher
                    .worker_manager
                    .submit_task_to_worker(task.task, worker_id);

                let task_future = async move {
                    tokio::select! {
                        biased;
                        // If the task was cancelled, return None
                        () = task.cancel_token.cancelled() => None,
                        // If the task is finished, return the result and the result_tx
                        result = task_handle.get_result() => {
                            Some((result, task.result_tx))
                        }
                    }
                };
                pending_tasks.spawn(task_future);
            }

            let num_pending_tasks = pending_tasks.len();

            tokio::select! {
                biased;
                Some(result) = pending_tasks.join_next(), if num_pending_tasks > 0 => {
                    match result {
                        Ok(Some((result, result_tx))) => {
                            let _ = result_tx.send(result);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            return Err(DaftError::External(e.into()));
                        }
                    }
                }
                true = async {
                    let limit = Self::MAX_TASKS_IN_BUFFER - queued_tasks.len();
                    let num_received = task_rx.recv_many(&mut queued_tasks, limit).await;
                    num_received > 0
                } => {}
                else => {
                    break;
                }
            }
        }

        dispatcher.worker_manager.shutdown()
    }
}

struct DispatchableTask {
    task: SwordfishTask,
    result_tx: OneshotSender<DaftResult<PartitionRef>>,
    cancel_token: CancellationToken,
}

#[derive(Clone, Debug)]
pub(crate) struct TaskDispatcherHandle {
    task_dispatcher_sender: Sender<DispatchableTask>,
}

impl TaskDispatcherHandle {
    fn new(task_dispatcher_sender: Sender<DispatchableTask>) -> Self {
        Self {
            task_dispatcher_sender,
        }
    }

    #[allow(dead_code)]
    pub async fn submit_task(&self, task: SwordfishTask) -> DaftResult<SubmittedTask> {
        let unsubmitted_task = UnsubmittedTask::new(task);
        let submitted_task = unsubmitted_task
            .submit_task(&self.task_dispatcher_sender)
            .await;
        Ok(submitted_task)
    }
}

#[allow(dead_code)]
struct UnsubmittedTask {
    task: SwordfishTask,
}

impl UnsubmittedTask {
    fn new(task: SwordfishTask) -> Self {
        Self { task }
    }

    async fn submit_task(self, task_dispatcher_sender: &Sender<DispatchableTask>) -> SubmittedTask {
        let (result_tx, result_rx) = create_oneshot_channel();
        let cancel_token = CancellationToken::new();
        let _ = task_dispatcher_sender
            .send(DispatchableTask {
                task: self.task,
                result_tx,
                cancel_token: cancel_token.clone(),
            })
            .await;
        SubmittedTask::new(result_rx, Some(cancel_token))
    }
}

pub(crate) struct SubmittedTask {
    result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
    cancel_token: Option<CancellationToken>,
}

impl SubmittedTask {
    fn new(
        result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            result_rx,
            cancel_token,
        }
    }
}

impl Future for SubmittedTask {
    type Output = Option<DaftResult<PartitionRef>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result_rx.poll_unpin(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(Some(result)),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for SubmittedTask {
    fn drop(&mut self) {
        if let Some(cancel_token) = self.cancel_token.take() {
            cancel_token.cancel();
        }
    }
}
