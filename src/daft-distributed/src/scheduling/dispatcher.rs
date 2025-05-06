use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use futures::FutureExt;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;

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
    pub fn new(worker_manager: Box<dyn WorkerManager>) -> Self {
        Self { worker_manager }
    }

    pub fn spawn_task_dispatcher(
        task_dispatcher: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> TaskDispatcherHandle {
        // TODO: Think about what the channel buffer size should be here.
        // In general, how much buffering of tasks can we allow
        // for the dispatcher?
        // To maximise throughput, we should never want the task dispatcher to have to wait for dispatchable tasks
        // to be picked up, if it has capacity to submit more tasks.
        // But if it does not have capacity, how much buffering should we allow?
        // In theory, tasks simply consist of a reference to a plan, as well as a partition ref.
        // So they should be quite lightweight, and it should be safe to buffer a lot of them.
        let (task_dispatcher_sender, task_dispatcher_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        TaskDispatcherHandle::new(task_dispatcher_sender)
    }

    // Determine which tasks can be scheduled on which workers
    // Returns a list of (task_idx, worker_id) tuples
    fn get_schedulable_task_and_worker_ids(
        queued_tasks: &mut [DispatchableTask],
        worker_manager: &dyn WorkerManager,
    ) -> Vec<(usize, String)> {
        // Sort tasks by estimated memory cost in ascending order
        // This should allow us to schedule tasks with the least memory requirements first,
        // which should help prevent resource fragmentation and improve throughput.
        queued_tasks.sort_by_key(|task| task.task.estimated_memory_cost());

        // Get worker resources and idle workers
        let worker_resources = worker_manager.get_worker_resources();
        let idle_workers = worker_manager.get_idle_workers();

        let mut assigned_workers = HashSet::new();
        let mut schedulable_task_and_worker_ids = Vec::new();

        for (i, task) in queued_tasks.iter().enumerate() {
            let task_memory = task.task.estimated_memory_cost();

            // Find best available worker that can handle this task
            if let Some((idx, (worker_id, _, _))) = worker_resources
                .iter()
                .enumerate()
                .filter(|(idx, (worker_id, _, worker_memory))| {
                    // Worker must not already be assigned in this batch
                    if assigned_workers.contains(idx) {
                        return false;
                    }

                    // If worker has enough memory, it can always accept the task
                    if *worker_memory >= task_memory {
                        return true;
                    }

                    // If worker doesn't have enough memory, it can only accept task if it's idle
                    idle_workers.contains(worker_id)
                })
                .min_by(|(_, (_, _, worker_memory1)), (_, (_, _, worker_memory2))| {
                    // Case 1: Both workers have sufficient memory
                    // Choose the one with the least excess (best fit)
                    if *worker_memory1 >= task_memory && *worker_memory2 >= task_memory {
                        return (*worker_memory1).cmp(worker_memory2);
                    }

                    // Case 2: Only first worker has sufficient memory
                    // First worker wins (sufficient always beats insufficient)
                    if *worker_memory1 >= task_memory {
                        return std::cmp::Ordering::Less;
                    }

                    // Case 3: Only second worker has sufficient memory
                    // Second worker wins (sufficient always beats insufficient)
                    if *worker_memory2 >= task_memory {
                        return std::cmp::Ordering::Greater;
                    }

                    // Case 4: Neither has sufficient memory
                    // Compare by smallest deficit (closest to requirement)
                    let deficit1 = task_memory - *worker_memory1;
                    let deficit2 = task_memory - *worker_memory2;
                    deficit1.cmp(&deficit2)
                })
            {
                // We found a worker, mark it as assigned
                assigned_workers.insert(idx);
                schedulable_task_and_worker_ids.push((i, worker_id.clone()));
            }
        }

        schedulable_task_and_worker_ids
    }

    async fn run_dispatch_loop(
        dispatcher: Self,
        task_rx: Receiver<DispatchableTask>,
    ) -> DaftResult<()> {
        let mut pending_tasks = JoinSet::new();
        let mut task_stream = ReceiverStream::new(task_rx);
        let mut queued_tasks: Vec<DispatchableTask> = Vec::new();

        loop {
            // Get schedulable tasks
            let schedulable_task_and_worker_ids = Self::get_schedulable_task_and_worker_ids(
                &mut queued_tasks,
                &*dispatcher.worker_manager,
            );

            // Schedule the tasks we found workers for (in reverse order to maintain indices, because we do swap_remove)
            for (task_idx, worker_id) in schedulable_task_and_worker_ids.into_iter().rev() {
                let task = queued_tasks.swap_remove(task_idx);
                let mut task_handle = dispatcher
                    .worker_manager
                    .submit_task_to_worker(task.task, worker_id);

                pending_tasks.spawn(async move {
                    tokio::select! {
                        biased;
                        () = task.cancel_token.cancelled() => None,
                        result = task_handle.get_result() => {
                            Some((result, task.result_tx))
                        }
                    }
                });
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
                            eprintln!("Error in task: {:?}", e);
                        }
                    }
                }
                Some(new_task) = task_stream.next() => {
                    queued_tasks.push(new_task);
                }
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

#[derive(Clone)]
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
