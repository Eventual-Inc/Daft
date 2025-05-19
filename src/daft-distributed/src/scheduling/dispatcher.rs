use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};

use super::{
    scheduler::{ScheduledTask, WorkerSnapshot},
    task::{Task, TaskId},
    worker::{Worker, WorkerId, WorkerManager},
};
use crate::utils::{
    channel::{create_channel, Receiver, Sender},
    joinset::{JoinSet, JoinSetId},
};

#[allow(dead_code)]
pub(super) struct DispatcherActor<W: Worker> {
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
        let (worker_update_sender, worker_update_receiver) = create_channel(1);
        joinset.spawn(Self::run_dispatcher_loop(
            dispatcher.worker_manager,
            dispatcher_receiver,
            worker_update_sender,
        ));
        DispatcherHandle::new(dispatcher_sender, worker_update_receiver)
    }

    fn dispatch_tasks(
        scheduled_tasks: Vec<ScheduledTask<W::Task>>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
        running_tasks: &mut JoinSet<()>,
        running_tasks_by_id: &mut HashMap<JoinSetId, (TaskId, WorkerId)>,
    ) -> DaftResult<()> {
        let mut worker_to_tasks = HashMap::new();

        for scheduled_task in scheduled_tasks {
            let (worker_id, task) = scheduled_task.into_inner();
            worker_to_tasks
                .entry(worker_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        let result_handles = worker_manager.submit_tasks_to_workers(worker_to_tasks)?;
        for result_handle in result_handles {
            let (task_id, worker_id) = (
                result_handle.task_id().clone(),
                result_handle.worker_id().clone(),
            );
            let id = running_tasks.spawn(result_handle.await_result());
            running_tasks_by_id.insert(id, (task_id, worker_id));
        }
        Ok(())
    }

    async fn handle_finished_task(
        finished_joinset_id: JoinSetId,
        finished_task_result: DaftResult<()>,
        running_tasks_by_id: &mut HashMap<JoinSetId, (TaskId, WorkerId)>,
        running_tasks: &mut JoinSet<()>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
        worker_update_sender: &Sender<Vec<WorkerSnapshot>>,
    ) -> DaftResult<()> {
        // Remove the first task from the running_tasks_by_id map
        finished_task_result?;
        let (task_id, worker_id) = running_tasks_by_id
            .remove(&finished_joinset_id)
            .expect("Task should be present in running_tasks_by_id");
        worker_manager.mark_task_finished(&task_id, &worker_id);

        // Try to get any other finished tasks
        while let Some((id, finished_task_result)) = running_tasks.try_join_next_with_id() {
            finished_task_result?;
            let (task_id, worker_id) = running_tasks_by_id
                .remove(&id)
                .expect("Task should be present in running_tasks_by_id");
            worker_manager.mark_task_finished(&task_id, &worker_id);
        }

        let workers = worker_manager.workers();
        let worker_snapshots = workers
            .values()
            .map(|w| WorkerSnapshot::new(w))
            .collect::<Vec<_>>();
        if worker_update_sender.send(worker_snapshots).await.is_err() {
            tracing::debug!("Unable to send worker update, dispatcher handle dropped");
        }

        Ok(())
    }

    async fn run_dispatcher_loop(
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        mut task_rx: Receiver<Vec<ScheduledTask<W::Task>>>,
        worker_update_sender: Sender<Vec<WorkerSnapshot>>,
    ) -> DaftResult<()> {
        let mut input_exhausted = false;
        let mut running_tasks = JoinSet::new();
        let mut running_tasks_by_id = HashMap::new();
        while !input_exhausted && !running_tasks.is_empty() {
            tokio::select! {
                maybe_tasks = task_rx.recv() => {
                    if let Some(tasks) = maybe_tasks {
                        Self::dispatch_tasks(
                            tasks,
                            &worker_manager,
                            &mut running_tasks,
                            &mut running_tasks_by_id,
                        )?;
                    } else {
                        input_exhausted = true;
                    }
                }
                Some((id, finished_task_result)) = running_tasks.join_next_with_id() => {
                     Self::handle_finished_task(
                        id,
                        finished_task_result,
                        &mut running_tasks_by_id,
                        &mut running_tasks,
                        &worker_manager,
                        &worker_update_sender,
                    ).await?;
                }
            }
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub(super) struct DispatcherHandle<T: Task> {
    dispatcher_sender: Sender<Vec<ScheduledTask<T>>>,
    worker_update_receiver: Receiver<Vec<WorkerSnapshot>>,
}

#[allow(dead_code)]
impl<T: Task> DispatcherHandle<T> {
    fn new(
        dispatcher_sender: Sender<Vec<ScheduledTask<T>>>,
        worker_update_receiver: Receiver<Vec<WorkerSnapshot>>,
    ) -> Self {
        Self {
            dispatcher_sender,
            worker_update_receiver,
        }
    }

    pub async fn dispatch_tasks(&self, tasks: Vec<ScheduledTask<T>>) -> DaftResult<()> {
        self.dispatcher_sender.send(tasks).await.map_err(|_| {
            DaftError::InternalError("Failed to send tasks to dispatcher".to_string())
        })?;
        Ok(())
    }

    pub async fn await_worker_updates(&mut self) -> Option<Vec<WorkerSnapshot>> {
        self.worker_update_receiver.recv().await
    }
}
