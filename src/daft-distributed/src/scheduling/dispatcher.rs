use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use super::{
    scheduler::ScheduledTask,
    task::{Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager},
};
use crate::utils::{
    channel::{create_channel, Receiver, Sender},
    joinset::JoinSet,
};

#[allow(dead_code)]
pub(crate) enum TaskStatus {
    Finished(WorkerId, TaskId),
    Cancelled(WorkerId, TaskId),
}

#[allow(dead_code)]
pub(crate) struct DispatcherActor<W: Worker> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker> DispatcherActor<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    pub fn spawn_dispatcher_actor(
        dispatcher: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> DispatcherHandle<W::Task> {
        let (dispatcher_sender, dispatcher_receiver) = create_channel(1);
        let (task_status_sender, task_status_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatcher_loop(
            dispatcher.worker_manager,
            dispatcher_receiver,
            task_status_sender,
        ));
        DispatcherHandle::new(dispatcher_sender, task_status_receiver)
    }

    async fn run_dispatcher_loop(
        _worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        _task_rx: Receiver<Vec<ScheduledTask<W::Task>>>,
        _task_status_sender: Sender<TaskStatus>,
    ) -> DaftResult<()> {
        todo!("FLOTILLA_MS1: Implement run scheduler loop");
    }
}

#[allow(dead_code)]
pub(crate) struct DispatcherHandle<T: Task> {
    dispatcher_sender: Sender<Vec<ScheduledTask<T>>>,
    task_status_receiver: Receiver<TaskStatus>,
}

#[allow(dead_code)]
impl<T: Task> DispatcherHandle<T> {
    fn new(
        dispatcher_sender: Sender<Vec<ScheduledTask<T>>>,
        task_status_receiver: Receiver<TaskStatus>,
    ) -> Self {
        Self {
            dispatcher_sender,
            task_status_receiver,
        }
    }

    async fn dispatch_tasks(&self, tasks: Vec<ScheduledTask<T>>) -> DaftResult<()> {
        self.dispatcher_sender.send(tasks).await.map_err(|_| {
            DaftError::InternalError("Failed to send tasks to dispatcher".to_string())
        })?;
        Ok(())
    }

    async fn await_task_status(&mut self) -> Option<TaskStatus> {
        self.task_status_receiver.recv().await
    }
}
