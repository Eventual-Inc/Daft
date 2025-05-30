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
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>();
        if worker_update_sender.send(worker_snapshots).await.is_err() {
            tracing::debug!("Unable to send worker update, dispatcher handle dropped");
        }

        Ok(())
    }

    #[tracing::instrument(name = "FlotillaDispatcher", skip_all)]
    async fn run_dispatcher_loop(
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        mut task_rx: Receiver<Vec<ScheduledTask<W::Task>>>,
        worker_update_sender: Sender<Vec<WorkerSnapshot>>,
    ) -> DaftResult<()>
    where
        <W as Worker>::TaskResultHandle: std::marker::Send,
    {
        let mut input_exhausted = false;
        let mut running_tasks = JoinSet::new();
        let mut running_tasks_by_id = HashMap::new();

        while !input_exhausted || !running_tasks.is_empty() {
            tokio::select! {
                maybe_tasks = task_rx.recv() => {
                    if let Some(tasks) = maybe_tasks {
                        tracing::debug!("Received new tasks: {:?}", tasks);

                        Self::dispatch_tasks(
                            tasks,
                            &worker_manager,
                            &mut running_tasks,
                            &mut running_tasks_by_id,
                        )?;
                    } else if !input_exhausted {
                        tracing::debug!("Input exhausted");

                        input_exhausted = true;
                    }
                }
                Some((id, finished_task_result)) = running_tasks.join_next_with_id() => {
                    tracing::debug!("Finished task: {:?}", id);

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

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, Rng, SeedableRng};

    use super::*;
    use crate::{
        scheduling::{
            scheduler::{test_utils::setup_workers, SchedulerHandle, SubmittedTask},
            task::tests::MockTaskFailure,
            tests::{create_mock_partition_ref, MockTask, MockTaskBuilder},
            worker::tests::MockWorkerManager,
        },
        utils::channel::create_oneshot_channel,
    };

    struct DispatcherTestContext {
        dispatcher_handle: DispatcherHandle<MockTask>,
        joinset: JoinSet<DaftResult<()>>,
    }

    fn setup_dispatcher_test_context(
        worker_configs: &[(WorkerId, usize)],
    ) -> DispatcherTestContext {
        let workers = setup_workers(worker_configs);
        let worker_manager = Arc::new(MockWorkerManager::new(workers));

        let dispatcher = DispatcherActor::new(worker_manager);
        let mut joinset = JoinSet::new();
        let dispatcher_handle = DispatcherActor::spawn_dispatcher_actor(dispatcher, &mut joinset);
        DispatcherTestContext {
            dispatcher_handle,
            joinset,
        }
    }

    impl DispatcherTestContext {
        async fn cleanup(mut self) -> DaftResult<()> {
            drop(self.dispatcher_handle);
            while let Some(task_result) = self.joinset.join_next().await {
                task_result??;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_dispatcher_actor_basic_task() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let test_context = setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let task = MockTaskBuilder::new(partition_ref.clone()).build();
        let (schedulable_task, submitted_task) = SchedulerHandle::prepare_task_for_submission(task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await
            .unwrap();

        let result = submitted_task.await?;
        let partition = result[0].partition();
        assert!(Arc::ptr_eq(&partition, &partition_ref));

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_actor_multiple_tasks() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let mut test_context = setup_dispatcher_test_context(&[(worker_id.clone(), 4)]);

        let num_tasks = 1000;

        let mut rng = StdRng::from_entropy();
        let (scheduled_tasks, submitted_tasks) = (0..num_tasks)
            .map(|i| {
                let task = MockTaskBuilder::new(create_mock_partition_ref(100 + i, 1024 * (i + 1)))
                    .with_task_id(format!("task-{}", i).into())
                    .with_sleep_duration(std::time::Duration::from_millis(rng.gen_range(100..200)))
                    .build();
                let (schedulable_task, submitted_task) =
                    SchedulerHandle::prepare_task_for_submission(task);
                (
                    ScheduledTask::new(schedulable_task, worker_id.clone()),
                    submitted_task,
                )
            })
            .unzip::<_, _, Vec<ScheduledTask<MockTask>>, Vec<SubmittedTask>>();

        test_context.joinset.spawn(async move {
            let mut count = 0;
            for (i, submitted_task) in submitted_tasks.into_iter().enumerate() {
                let result = submitted_task.await?;
                let partition = result[0].partition();
                assert_eq!(partition.num_rows().unwrap(), 100 + i);
                assert_eq!(partition.size_bytes().unwrap(), Some(1024 * (i + 1)));
                count += 1;
            }
            assert_eq!(count, num_tasks);
            Ok(())
        });

        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await?;

        test_context.cleanup().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_actor_cancelled_task() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let test_context = setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let (cancel_notifier, cancel_receiver) = create_oneshot_channel();
        let task = MockTaskBuilder::new(partition_ref.clone())
            .with_cancel_notifier(cancel_notifier)
            .build();
        let (schedulable_task, submitted_task) = SchedulerHandle::prepare_task_for_submission(task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await
            .unwrap();

        drop(submitted_task);
        cancel_receiver.await.unwrap();

        test_context.cleanup().await?;
        Ok(())
    }
    #[tokio::test]
    async fn test_task_error_basic() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let test_context = setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Error("test error".to_string()))
            .build();
        let (schedulable_task, submitted_task) = SchedulerHandle::prepare_task_for_submission(task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await?;

        let result = submitted_task.await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "DaftError::InternalError test error"
        );

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_task_panic_basic() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let test_context = setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Panic("test panic".to_string()))
            .build();

        let (schedulable_task, submitted_task) = SchedulerHandle::prepare_task_for_submission(task);
        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await?;

        let result = submitted_task.await?;
        assert_eq!(result.len(), 0);

        let text_context_result = test_context.cleanup().await;
        assert!(text_context_result.is_err());
        assert!(text_context_result
            .unwrap_err()
            .to_string()
            .contains("panicked with message \"test panic\""));

        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_drops_if_task_panics() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let test_context = setup_dispatcher_test_context(&[(worker_id.clone(), 2)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Panic("test panic".to_string()))
            .build();

        let (schedulable_task, submitted_task) = SchedulerHandle::prepare_task_for_submission(task);
        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id.clone())];
        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await?;
        let result = submitted_task.await?;
        assert_eq!(result.len(), 0);

        let new_task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024)).build();

        let (schedulable_task, _submitted_task) =
            SchedulerHandle::prepare_task_for_submission(new_task);
        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        let result = test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await;
        assert!(result.is_err());
        let error_message = result.err().unwrap().to_string();
        assert_eq!(
            error_message,
            "DaftError::InternalError Failed to send tasks to dispatcher"
        );

        let cleanup_result = test_context.cleanup().await;
        assert!(cleanup_result.is_err());
        assert!(cleanup_result
            .unwrap_err()
            .to_string()
            .contains("test panic"));

        Ok(())
    }
}
