use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::FutureExt;

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
        let (task_dispatcher_sender, task_dispatcher_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        TaskDispatcherHandle::new(task_dispatcher_sender)
    }

    async fn run_dispatch_loop(
        _dispatcher: Self,
        _task_rx: Receiver<DispatchableTask>,
    ) -> DaftResult<()> {
        todo!()
    }
}

#[derive(Clone)]
pub struct TaskDispatcherHandle {
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
        let (result_tx, result_rx) = create_oneshot_channel();
        let (cancel_tx, cancel_rx) = create_oneshot_channel();
        let dispatchable_task = DispatchableTask::new(task, result_tx, cancel_rx);

        self.task_dispatcher_sender
            .send(dispatchable_task)
            .await
            .map_err(|e| DaftError::InternalError(e.to_string()))?;

        Ok(SubmittedTask::new(result_rx, cancel_tx))
    }
}

#[allow(dead_code)]
struct DispatchableTask {
    // The task to dispatch
    task: SwordfishTask,
    // The sender to send the result into
    result_tx: OneshotSender<DaftResult<PartitionRef>>,
    // The receiver to receive the cancel signal
    cancel_rx: OneshotReceiver<()>,
}

impl DispatchableTask {
    fn new(
        task: SwordfishTask,
        result_tx: OneshotSender<DaftResult<PartitionRef>>,
        cancel_rx: OneshotReceiver<()>,
    ) -> Self {
        Self {
            task,
            result_tx,
            cancel_rx,
        }
    }
}

pub struct SubmittedTask {
    result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
    cancel_tx: Option<OneshotSender<()>>,
}

impl SubmittedTask {
    fn new(
        result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
        cancel_tx: OneshotSender<()>,
    ) -> Self {
        Self {
            result_rx,
            cancel_tx: Some(cancel_tx),
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
        if let Some(cancel_tx) = self.cancel_tx.take() {
            if !cancel_tx.is_closed() {
                let _ = cancel_tx.send(());
            }
        }
    }
}
