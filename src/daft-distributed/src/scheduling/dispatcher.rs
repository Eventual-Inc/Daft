use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::FutureExt;
use tokio_stream::{adapters::Peekable, wrappers::ReceiverStream, StreamExt};
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

    async fn get_next_task_and_worker_id(
        task_stream: &mut Peekable<ReceiverStream<DispatchableTask>>,
        worker_manager: &dyn WorkerManager,
    ) -> Option<(DispatchableTask, String)> {
        let worker_id = match task_stream.peek().await {
            Some(task) => {
                let task_memory_cost = task.task.estimated_memory_cost();
                let available_resources = worker_manager.get_worker_resources();
                available_resources
                    .iter()
                    .find(|(_, _, memory)| *memory >= task_memory_cost)
                    .map(|(worker_id, _, _)| worker_id.clone())
            }
            None => None,
        };
        if let Some(worker_id) = worker_id {
            let task = task_stream.next().await.unwrap();
            Some((task, worker_id))
        } else {
            None
        }
    }

    async fn run_dispatch_loop(
        dispatcher: Self,
        task_rx: Receiver<DispatchableTask>,
    ) -> DaftResult<()> {
        let mut pending_tasks = JoinSet::new();
        let mut task_stream = ReceiverStream::new(task_rx).peekable();
        loop {
            let num_pending_tasks = pending_tasks.len();
            tokio::select! {
                biased;
                Some((dispatchable_task, worker_id)) = Self::get_next_task_and_worker_id(&mut task_stream, &*dispatcher.worker_manager) => {
                    let mut task_handle = dispatcher
                        .worker_manager
                        .submit_task_to_worker(dispatchable_task.task, worker_id);
                    pending_tasks.spawn(async move {
                        tokio::select! {
                            biased;
                            () = dispatchable_task.cancel_token.cancelled() => None,
                            result = task_handle.get_result() => {
                                Some((result, dispatchable_task.result_tx))
                            }
                        }
                    });
                }
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
                else => {
                    break;
                }
            }
        }
        dispatcher.worker_manager.shutdown()
    }
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

    async fn dispatch_task(&self, task: SwordfishTask) -> DaftResult<SubmittedTask> {
        let (result_tx, result_rx) = create_oneshot_channel();
        let cancel_token = CancellationToken::new();
        let dispatchable_task = DispatchableTask::new(task, result_tx, cancel_token.clone());
        self.task_dispatcher_sender
            .send(dispatchable_task)
            .await
            .map_err(|e| DaftError::InternalError(e.to_string()))?;
        Ok(SubmittedTask::new(result_rx, Some(cancel_token)))
    }

    #[allow(dead_code)]
    pub async fn submit_task(&self, task: SwordfishTask) -> DaftResult<SubmittedTask> {
        self.dispatch_task(task).await
    }
}

#[allow(dead_code)]
struct DispatchableTask {
    task: SwordfishTask,
    result_tx: OneshotSender<DaftResult<PartitionRef>>,
    cancel_token: CancellationToken,
}

impl DispatchableTask {
    fn new(
        task: SwordfishTask,
        result_tx: OneshotSender<DaftResult<PartitionRef>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task,
            result_tx,
            cancel_token,
        }
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
