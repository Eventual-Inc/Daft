use std::{collections::HashMap, sync::Arc, time::Duration};

use common_error::{DaftError, DaftResult};

use super::{
    scheduler::{ScheduledTask, WorkerSnapshot},
    task::Task,
    worker::{Worker, WorkerManager},
};
use crate::{
    scheduling::{
        scheduler::{SchedulableTask, SchedulerSender},
        task::{TaskContext, TaskResult, TaskResultAwaiter, TaskResultHandle, TaskStatus},
    },
    statistics::{StatisticsEvent, StatisticsManagerRef},
    utils::{
        channel::{
            create_channel, create_watch_channel, Receiver, Sender, WatchReceiver, WatchSender,
        },
        joinset::JoinSet,
    },
};

pub(super) struct DispatcherActor<W: Worker> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker> DispatcherActor<W> {
    const DISPATCHER_TICK_INTERVAL: Duration = Duration::from_secs(1);

    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    pub fn spawn_dispatcher_actor(
        dispatcher: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
        statistics_manager: StatisticsManagerRef,
        failed_task_sender: SchedulerSender<W::Task>,
    ) -> DispatcherHandle<W::Task> {
        let (dispatcher_sender, dispatcher_receiver) = create_channel(1);
        let (worker_update_sender, worker_update_receiver) = create_watch_channel(vec![]);
        joinset.spawn(Self::run_dispatcher_loop(
            dispatcher.worker_manager,
            dispatcher_receiver,
            worker_update_sender,
            failed_task_sender,
            statistics_manager,
        ));
        DispatcherHandle::new(dispatcher_sender, worker_update_receiver)
    }

    fn dispatch_tasks(
        scheduled_tasks: Vec<ScheduledTask<W::Task>>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
        task_result_futures: &mut JoinSet<TaskResult>,
        running_tasks: &mut HashMap<TaskContext, ScheduledTask<W::Task>>,
    ) -> DaftResult<()> {
        let mut worker_to_tasks = HashMap::new();

        for scheduled_task in scheduled_tasks {
            let worker_id = scheduled_task.worker_id();
            let task = scheduled_task.task();
            running_tasks.insert(task.task_context(), scheduled_task);
            worker_to_tasks
                .entry(worker_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        let result_handles = worker_manager.submit_tasks_to_workers(worker_to_tasks)?;
        for result_handle in result_handles {
            let scheduled_task = running_tasks
                .get(&result_handle.task_context())
                .expect("Task should be present in running_tasks");
            let result_awaiter = TaskResultAwaiter::new(
                result_handle.task_context(),
                result_handle,
                scheduled_task.cancel_token(),
            );
            task_result_futures.spawn(result_awaiter.await_result());
        }
        Ok(())
    }

    async fn handle_finished_tasks(
        task_results: Vec<TaskResult>,
        running_tasks: &mut HashMap<TaskContext, ScheduledTask<W::Task>>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
        worker_update_sender: &WatchSender<Vec<WorkerSnapshot>>,
        failed_task_sender: &SchedulerSender<W::Task>,
        statistics_manager: &StatisticsManagerRef,
    ) -> DaftResult<()> {
        for (task_context, task_status) in task_results {
            let scheduled_task = running_tasks
                .remove(&task_context)
                .expect("Task should be present in running_tasks_by_id");
            let (worker_id, task, result_tx, canc) = scheduled_task.into_inner();
            match task_status {
                TaskStatus::Success { result } => {
                    worker_manager.mark_task_finished(task_context, worker_id);
                    statistics_manager.handle_event(StatisticsEvent::FinishedTask {
                        context: task_context,
                    })?;
                    let _ = result_tx.send(Ok(result));
                }
                TaskStatus::Failed { error } => {
                    worker_manager.mark_task_finished(task_context, worker_id);
                    let _ = result_tx.send(Err(error));
                }
                TaskStatus::Cancelled => {
                    worker_manager.mark_task_finished(task_context, worker_id);
                    statistics_manager.handle_event(StatisticsEvent::FinishedTask {
                        context: task_context,
                    })?;
                }
                TaskStatus::WorkerDied => {
                    worker_manager.mark_worker_died(worker_id);
                    let schedulable_task = SchedulableTask::new(task, result_tx, canc);
                    let _ = failed_task_sender.send(schedulable_task);
                }
                TaskStatus::WorkerUnavailable => {
                    let schedulable_task = SchedulableTask::new(task, result_tx, canc);
                    let _ = failed_task_sender.send(schedulable_task);
                }
            }
        }
        Self::handle_worker_updates(worker_manager, worker_update_sender).await?;
        Ok(())
    }

    async fn handle_worker_updates(
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
        worker_update_sender: &WatchSender<Vec<WorkerSnapshot>>,
    ) -> DaftResult<()> {
        let worker_snapshots = worker_manager.worker_snapshots()?;
        if worker_update_sender.send(worker_snapshots).is_err() {
            tracing::debug!("Unable to send worker update while handling worker updates, dispatcher handle dropped");
        }
        Ok(())
    }

    #[tracing::instrument(name = "FlotillaDispatcher", skip_all)]
    async fn run_dispatcher_loop(
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        mut task_rx: Receiver<Vec<ScheduledTask<W::Task>>>,
        worker_update_sender: WatchSender<Vec<WorkerSnapshot>>,
        failed_task_sender: SchedulerSender<W::Task>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<()>
    where
        <W as Worker>::TaskResultHandle: std::marker::Send,
    {
        // send worker updates at the start of the loop
        Self::handle_worker_updates(&worker_manager, &worker_update_sender).await?;
        let mut input_exhausted = false;
        let mut running_tasks = JoinSet::new();
        let mut running_tasks_by_context = HashMap::new();
        let mut tick_interval = tokio::time::interval(Self::DISPATCHER_TICK_INTERVAL);
        while !input_exhausted || !running_tasks.is_empty() {
            tokio::select! {
                maybe_tasks = task_rx.recv() => {
                    if let Some(tasks) = maybe_tasks {
                        Self::dispatch_tasks(
                            tasks,
                            &worker_manager,
                            &mut running_tasks,
                            &mut running_tasks_by_context,
                        )?;
                    } else if !input_exhausted {
                        input_exhausted = true;
                    }
                }
                Some(task_result) = running_tasks.join_next() => {
                    let mut task_results = vec![task_result?];
                    while let Some(task_result) = running_tasks.try_join_next() {
                        task_results.push(task_result?);
                    }
                    Self::handle_finished_tasks(
                        task_results,
                        &mut running_tasks_by_context,
                        &worker_manager,
                        &worker_update_sender,
                        &failed_task_sender,
                        &statistics_manager,
                    ).await?;
                }
                _ = tick_interval.tick() => {
                    Self::handle_worker_updates(&worker_manager, &worker_update_sender).await?;
                }
            }
        }
        Ok(())
    }
}

pub(super) struct DispatcherHandle<T: Task> {
    dispatcher_sender: Sender<Vec<ScheduledTask<T>>>,
    worker_update_receiver: WatchReceiver<Vec<WorkerSnapshot>>,
}

impl<T: Task> DispatcherHandle<T> {
    fn new(
        dispatcher_sender: Sender<Vec<ScheduledTask<T>>>,
        worker_update_receiver: WatchReceiver<Vec<WorkerSnapshot>>,
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
        self.worker_update_receiver.changed().await.ok()?;
        let worker_snapshots = self.worker_update_receiver.borrow_and_update().clone();
        Some(worker_snapshots)
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, Rng, SeedableRng};

    use super::*;
    use crate::{
        scheduling::{
            scheduler::{
                test_utils::setup_workers, SchedulerHandle, SubmittableTask, SubmittedTask,
            },
            task::tests::MockTaskFailure,
            tests::{create_mock_partition_ref, MockTask, MockTaskBuilder},
            worker::{tests::MockWorkerManager, WorkerId},
        },
        utils::channel::{create_oneshot_channel, create_unbounded_channel, UnboundedReceiver},
    };

    struct DispatcherTestContext {
        dispatcher_handle: DispatcherHandle<MockTask>,
        joinset: JoinSet<DaftResult<()>>,
        _failed_task_receiver: UnboundedReceiver<SchedulableTask<MockTask>>,
    }

    fn setup_dispatcher_test_context(
        worker_configs: &[(WorkerId, usize)],
    ) -> DispatcherTestContext {
        let workers = setup_workers(worker_configs);
        let worker_manager = Arc::new(MockWorkerManager::new(workers));

        let dispatcher = DispatcherActor::new(worker_manager);
        let mut joinset = JoinSet::new();
        let (failed_task_sender, failed_task_receiver) = create_unbounded_channel();
        let dispatcher_handle = DispatcherActor::spawn_dispatcher_actor(
            dispatcher,
            &mut joinset,
            StatisticsManagerRef::default(),
            failed_task_sender,
        );
        DispatcherTestContext {
            dispatcher_handle,
            joinset,
            _failed_task_receiver: failed_task_receiver,
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
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

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
                    .with_task_id(i as u32)
                    .with_sleep_duration(std::time::Duration::from_millis(rng.gen_range(100..200)))
                    .build();
                let submittable_task = SubmittableTask::new(task);
                let (schedulable_task, submitted_task) =
                    SchedulerHandle::prepare_task_for_submission(submittable_task);
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
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

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
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

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

        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);
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

        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);
        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id.clone())];
        test_context
            .dispatcher_handle
            .dispatch_tasks(scheduled_tasks)
            .await?;
        let result = submitted_task.await?;
        assert_eq!(result.len(), 0);

        let new_task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024)).build();

        let submittable_task = SubmittableTask::new(new_task);
        let (schedulable_task, _submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);
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
