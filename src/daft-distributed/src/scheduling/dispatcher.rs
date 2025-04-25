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
    runtime::{create_join_set, JoinSet},
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
    ) -> DaftResult<TaskDispatcherHandle> {
        let (task_dispatcher_sender, task_dispatcher_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatch_loop(
            task_dispatcher,
            task_dispatcher_receiver,
        ));
        Ok(TaskDispatcherHandle::new(task_dispatcher_sender))
    }

    pub async fn run_dispatch_loop(
        dispatcher: Self,
        mut task_rx: Receiver<TaskDispatchWrapper>,
    ) -> DaftResult<()> {
        let mut pending_tasks = create_join_set();
        loop {
            let next_available_worker = dispatcher.get_available_worker();
            let has_available_worker = next_available_worker.is_some();
            let num_pending_tasks = pending_tasks.len();
            tokio::select! {
                biased;
                Some(task_dispatch_wrapper) = task_rx.recv(), if has_available_worker => {
                    let task_handle = dispatcher
                        .worker_manager
                        .submit_task_to_worker(task_dispatch_wrapper.0, next_available_worker.unwrap());
                    pending_tasks.spawn(async move {
                        tokio::select! {
                            biased;
                            _ = task_dispatch_wrapper.2 => {
                                None
                            }
                            result = task_handle.get_result() => {
                                Some((result, task_dispatch_wrapper.1))
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
                            return Err(DaftError::InternalError(e.to_string()));
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        dispatcher.worker_manager.shutdown();
        Ok(())
    }

    pub fn get_available_worker(&self) -> Option<String> {
        let worker_resources = self.worker_manager.get_worker_resources();
        // get the worker with the most available memory
        worker_resources
            .into_iter()
            .max_by_key(|(_, _, memory)| *memory)
            .map(|(worker_id, _, _)| worker_id)
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
