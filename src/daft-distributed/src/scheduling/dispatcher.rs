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

pub type TaskDispatchWrapper = (
    // Task to dispatch
    SwordfishTask,
    // Result channel
    tokio::sync::oneshot::Sender<DaftResult<PartitionRef>>,
    // Cancel channel
    tokio::sync::oneshot::Receiver<()>,
);

// The task dispatcher is responsible for dispatching tasks to workers.
#[allow(dead_code)]
pub(crate) struct TaskDispatcher {
    worker_manager: Box<dyn WorkerManager>,
}

impl TaskDispatcher {
    #[allow(dead_code)]
    pub fn new(worker_manager: Box<dyn WorkerManager>) -> Self {
        Self { worker_manager }
    }

    #[allow(dead_code)]
    pub fn spawn_task_dispatcher(
        task_dispatcher: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> DaftResult<TaskDispatcherHandle> {
        let (task_dispatcher_sender, task_dispatcher_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        Ok(TaskDispatcherHandle::new(task_dispatcher_sender))
    }

    pub async fn run_dispatch_loop(
        _dispatcher: Self,
        _task_rx: Receiver<TaskDispatchWrapper>,
    ) -> DaftResult<()> {
        todo!()
    }
}

#[derive(Clone)]
pub struct TaskDispatcherHandle {
    task_dispatcher_sender: Sender<TaskDispatchWrapper>,
}

impl TaskDispatcherHandle {
    pub fn new(task_dispatcher_sender: Sender<TaskDispatchWrapper>) -> Self {
        Self {
            task_dispatcher_sender,
        }
    }

    #[allow(dead_code)]
    pub async fn submit_task(&self, task: SwordfishTask) -> DaftResult<TaskResultReceiver> {
        let (result_tx, result_rx) = create_oneshot_channel();
        let (cancel_tx, cancel_rx) = create_oneshot_channel();
        let task_dispatch_wrapper = (task, result_tx, cancel_rx);
        self.task_dispatcher_sender
            .send(task_dispatch_wrapper)
            .await
            .map_err(|e| DaftError::InternalError(e.to_string()))?;
        Ok(TaskResultReceiver::new(result_rx, cancel_tx))
    }
}

pub struct TaskResultReceiver {
    result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
    cancel_tx: Option<OneshotSender<()>>,
}

impl TaskResultReceiver {
    #[allow(dead_code)]
    pub fn new(
        result_rx: OneshotReceiver<DaftResult<PartitionRef>>,
        cancel_tx: OneshotSender<()>,
    ) -> Self {
        Self {
            result_rx,
            cancel_tx: Some(cancel_tx),
        }
    }
}

impl Future for TaskResultReceiver {
    type Output = Option<DaftResult<PartitionRef>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result_rx.poll_unpin(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(Some(result)),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for TaskResultReceiver {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            if !cancel_tx.is_closed() {
                let _ = cancel_tx.send(());
            }
        }
    }
}
