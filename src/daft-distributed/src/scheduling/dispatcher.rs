use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;

use super::{
    scheduler::{PendingTask, ScheduledTask},
    task::{Task, TaskResultAwaiter, TaskStatus},
    worker::{Worker, WorkerManager},
};
use crate::{
    scheduling::task::TaskResultHandle,
    statistics::StatisticsManagerRef,
    utils::joinset::{JoinSet, JoinSetId},
};

const DISPATCHER_LOG_TARGET: &str = "DaftFlotillaDispatcher";

pub(super) struct Dispatcher<W: Worker> {
    // JoinSet of task results futures
    task_result_joinset: JoinSet<TaskStatus>,
    // Mapping of joinset task id to the scheduled task
    // The scheduled task is kept here so that we can reschedule the task if it fails
    joinset_id_to_task: HashMap<JoinSetId, ScheduledTask<W::Task>>,
}

impl<W: Worker> Dispatcher<W> {
    pub fn new() -> Self {
        Self {
            task_result_joinset: JoinSet::new(),
            joinset_id_to_task: HashMap::new(),
        }
    }

    pub fn dispatch_tasks(
        &mut self,
        scheduled_tasks: Vec<ScheduledTask<W::Task>>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
    ) -> DaftResult<()> {
        let mut worker_to_tasks = HashMap::new();
        let mut task_context_to_task = HashMap::new();

        for scheduled_task in scheduled_tasks {
            let worker_id = scheduled_task.worker_id();
            let task = scheduled_task.task();
            task_context_to_task.insert(task.task_context(), scheduled_task);
            worker_to_tasks
                .entry(worker_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        let result_handles = worker_manager.submit_tasks_to_workers(worker_to_tasks)?;

        for result_handle in result_handles {
            let scheduled_task = task_context_to_task
                .remove(&result_handle.task_context())
                .expect("Task should be present in task_context_to_task");
            let result_awaiter =
                TaskResultAwaiter::new(result_handle, scheduled_task.cancel_token());
            let id = self
                .task_result_joinset
                .spawn(result_awaiter.await_result());
            self.joinset_id_to_task.insert(id, scheduled_task);
        }

        Ok(())
    }

    /// Await at least one completed task and return any failed tasks that need to be rescheduled.
    /// This method will block until at least one task completes, then poll for any additional
    /// completed tasks that are immediately available.
    pub async fn await_completed_tasks(
        &mut self,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
        statistics_manager: &StatisticsManagerRef,
    ) -> DaftResult<Vec<PendingTask<W::Task>>> {
        let mut failed_tasks = Vec::new();
        let mut task_results = Vec::new();

        // Wait for at least one task to complete
        if let Some((id, task_result)) = self.task_result_joinset.join_next_with_id().await {
            let scheduled_task = self
                .joinset_id_to_task
                .remove(&id)
                .expect("Task should be present in joinset_id_to_task");
            task_results.push(CompletedTask::new(task_result, scheduled_task));

            // Collect any additional completed tasks that are immediately available
            while let Some((id, task_result)) = self.task_result_joinset.try_join_next_with_id() {
                let scheduled_task = self
                    .joinset_id_to_task
                    .remove(&id)
                    .expect("Task should be present in joinset_id_to_task");
                task_results.push(CompletedTask::new(task_result, scheduled_task));
            }

            tracing::info!(target: DISPATCHER_LOG_TARGET, num_tasks = task_results.len(), "Awaited completed tasks");
            tracing::debug!(target: DISPATCHER_LOG_TARGET, completed_tasks = %format!("{:#?}", task_results));

            // Process all completed tasks
            for CompletedTask { task_result, task } in task_results {
                let (worker_id, task, result_tx, canc) = task.into_inner();

                // Always mark the task as finished regardless of the result
                worker_manager.mark_task_finished(task.task_context(), worker_id.clone());
                // Send the event to the statistics manager
                statistics_manager.handle_event((task.task_context(), &task_result).into())?;

                match task_result {
                    Ok(task_status) => match task_status {
                        // Task completed successfully, send the result to the result_tx
                        TaskStatus::Success { result } => {
                            if result_tx.send(Ok(Some(result))).is_err() {
                                tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send result of task to result_tx", task_context = ?task.task_context());
                            }
                        }
                        // Task failed, send the error to the result_tx
                        TaskStatus::Failed { error } => {
                            if result_tx.send(Err(error)).is_err() {
                                tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send error of task to result_tx", task_context = ?task.task_context());
                            }
                        }
                        // Task cancelled, do nothing
                        TaskStatus::Cancelled => {}
                        // Task worker died, add the task to the failed tasks, and mark the worker as dead
                        TaskStatus::WorkerDied => {
                            worker_manager.mark_worker_died(worker_id);
                            let schedulable_task = PendingTask::new(task, result_tx, canc);
                            failed_tasks.push(schedulable_task);
                        }
                        // Task worker unavailable, add the task to the failed tasks
                        TaskStatus::WorkerUnavailable => {
                            let schedulable_task = PendingTask::new(task, result_tx, canc);
                            failed_tasks.push(schedulable_task);
                        }
                    },
                    // Task failed because of panic in joinset, send the error to the result_tx
                    Err(e) => {
                        if result_tx.send(Err(e)).is_err() {
                            tracing::error!(target: DISPATCHER_LOG_TARGET, error = "Failed to send error of task to result_tx", task_context = ?task.task_context());
                        }
                    }
                }
            }
        }

        Ok(failed_tasks)
    }

    pub fn has_running_tasks(&self) -> bool {
        !self.task_result_joinset.is_empty()
    }
}

struct CompletedTask<T: Task> {
    task_result: DaftResult<TaskStatus>,
    task: ScheduledTask<T>,
}

impl<T: Task> CompletedTask<T> {
    fn new(task_result: DaftResult<TaskStatus>, task: ScheduledTask<T>) -> Self {
        Self { task_result, task }
    }
}

impl<T: Task> std::fmt::Debug for CompletedTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CompletedTask({:?}, {:?})",
            self.task.task().task_context(),
            self.task_result
        )
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
            task::{tests::MockTaskFailure, SchedulingStrategy},
            tests::{create_mock_partition_ref, MockTask, MockTaskBuilder},
            worker::{
                tests::{MockWorker, MockWorkerManager},
                WorkerId,
            },
        },
        utils::channel::create_oneshot_channel,
    };

    fn setup_dispatcher_test_context(
        worker_configs: &[(WorkerId, usize)],
    ) -> (
        Dispatcher<MockWorker>,
        Arc<dyn WorkerManager<Worker = MockWorker>>,
    ) {
        let workers = setup_workers(worker_configs);
        let worker_manager: Arc<dyn WorkerManager<Worker = MockWorker>> =
            Arc::new(MockWorkerManager::new(workers));
        (Dispatcher::new(), worker_manager)
    }

    #[tokio::test]
    async fn test_dispatcher_basic_task() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let task = MockTaskBuilder::new(partition_ref.clone()).build();
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        // Wait for task completion
        let failed_tasks = dispatcher
            .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
            .await?;
        assert!(failed_tasks.is_empty());

        let result = submitted_task.await?;
        let partition = result.unwrap().partitions()[0].clone();
        assert!(Arc::ptr_eq(&partition, &partition_ref));

        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_multiple_tasks() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 4)]);

        let num_tasks = 100;
        let mut rng = StdRng::from_entropy();
        let (scheduled_tasks, submitted_tasks) = (0..num_tasks)
            .map(|i| {
                let task = MockTaskBuilder::new(create_mock_partition_ref(100 + i, 1024 * (i + 1)))
                    .with_task_id(i as u32)
                    .with_sleep_duration(std::time::Duration::from_millis(rng.gen_range(50..100)))
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

        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        // Wait for all tasks to complete
        while dispatcher.has_running_tasks() {
            let failed_tasks = dispatcher
                .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
                .await?;
            assert!(failed_tasks.is_empty());
        }

        // Verify results
        for (i, submitted_task) in submitted_tasks.into_iter().enumerate() {
            let result = submitted_task.await?;
            let partition = result.unwrap().partitions()[0].clone();
            assert_eq!(partition.num_rows().unwrap(), 100 + i);
            assert_eq!(partition.size_bytes().unwrap(), Some(1024 * (i + 1)));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_cancelled_task() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let (cancel_notifier, cancel_receiver) = create_oneshot_channel();
        let task = MockTaskBuilder::new(partition_ref.clone())
            .with_cancel_notifier(cancel_notifier)
            .build();
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        drop(submitted_task);
        cancel_receiver.await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_task_error_basic() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Error("test error".to_string()))
            .build();
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher
            .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
            .await?;
        assert!(failed_tasks.is_empty());

        let result = submitted_task.await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "DaftError::InternalError test error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_task_panic_basic() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::Panic("test panic".to_string()))
            .build();

        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);
        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id)];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher
            .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
            .await?;
        assert!(failed_tasks.is_empty());

        let result = submitted_task.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test panic"));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_worker_died() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        // Verify worker is initially present
        let initial_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(initial_snapshots.len(), 1);
        assert_eq!(initial_snapshots[0].worker_id(), &worker_id);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::WorkerDied)
            .build();
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, _submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id.clone())];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher
            .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
            .await?;

        // Task should be returned as a failed task that needs rescheduling
        assert_eq!(failed_tasks.len(), 1);
        let failed_task = &failed_tasks[0];
        assert_eq!(failed_task.task_context().task_id, 0);

        // Verify that the worker that died is no longer in the worker snapshots
        let worker_snapshots = worker_manager.worker_snapshots()?;
        assert!(
            worker_snapshots.is_empty(),
            "Dead worker should not appear in worker snapshots"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_task_worker_unavailable() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let (mut dispatcher, worker_manager) =
            setup_dispatcher_test_context(&[(worker_id.clone(), 1)]);

        // Verify worker is initially present
        let initial_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(initial_snapshots.len(), 1);
        assert_eq!(initial_snapshots[0].worker_id(), &worker_id);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
            .with_failure(MockTaskFailure::WorkerUnavailable)
            .build();
        let submittable_task = SubmittableTask::new(task);
        let (schedulable_task, _submitted_task) =
            SchedulerHandle::prepare_task_for_submission(submittable_task);

        let scheduled_tasks = vec![ScheduledTask::new(schedulable_task, worker_id.clone())];
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        let failed_tasks = dispatcher
            .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
            .await?;

        // Task should be returned as a failed task that needs rescheduling
        assert_eq!(failed_tasks.len(), 1);
        let failed_task = &failed_tasks[0];
        assert_eq!(failed_task.task_context().task_id, 0);

        // Verify that the worker is still present in snapshots (unavailable != dead)
        let worker_snapshots = worker_manager.worker_snapshots()?;
        assert_eq!(
            worker_snapshots.len(),
            1,
            "Worker should still be present when unavailable"
        );
        assert_eq!(worker_snapshots[0].worker_id(), &worker_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_failed_tasks() -> DaftResult<()> {
        // Use multiple workers to avoid interference between tasks
        let worker1_id: WorkerId = Arc::from("worker1");
        let worker2_id: WorkerId = Arc::from("worker2");
        let worker3_id: WorkerId = Arc::from("worker3");
        let (mut dispatcher, worker_manager) = setup_dispatcher_test_context(&[
            (worker1_id.clone(), 1),
            (worker2_id.clone(), 1),
            (worker3_id.clone(), 1),
        ]);

        let tasks = vec![
            MockTaskBuilder::new(create_mock_partition_ref(100, 1024))
                .with_task_id(1)
                .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker1_id.clone(),
                    soft: false,
                })
                .with_failure(MockTaskFailure::WorkerDied)
                .build(),
            MockTaskBuilder::new(create_mock_partition_ref(200, 2048))
                .with_task_id(2)
                .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker2_id.clone(),
                    soft: false,
                })
                .with_failure(MockTaskFailure::WorkerUnavailable)
                .build(),
            MockTaskBuilder::new(create_mock_partition_ref(300, 3072))
                .with_task_id(3)
                .with_scheduling_strategy(SchedulingStrategy::WorkerAffinity {
                    worker_id: worker3_id.clone(),
                    soft: false,
                })
                .with_failure(MockTaskFailure::WorkerDied)
                .build(),
        ];

        let (scheduled_tasks, _submitted_tasks) = tasks
            .into_iter()
            .zip(vec![
                worker1_id.clone(),
                worker2_id.clone(),
                worker3_id.clone(),
            ])
            .map(|(task, worker_id)| {
                let submittable_task = SubmittableTask::new(task);
                let (schedulable_task, submitted_task) =
                    SchedulerHandle::prepare_task_for_submission(submittable_task);
                (
                    ScheduledTask::new(schedulable_task, worker_id),
                    submitted_task,
                )
            })
            .unzip::<_, _, Vec<ScheduledTask<MockTask>>, Vec<SubmittedTask>>();

        // Dispatch all tasks at once
        dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;

        // Wait for all tasks to complete and collect failed tasks
        let mut all_failed_tasks = Vec::new();
        while dispatcher.has_running_tasks() {
            let failed_tasks = dispatcher
                .await_completed_tasks(&worker_manager, &StatisticsManagerRef::default())
                .await?;
            all_failed_tasks.extend(failed_tasks);
        }

        // All tasks should be returned as failed tasks that need rescheduling
        assert_eq!(all_failed_tasks.len(), 3);

        // Check that we have all the expected task IDs
        let mut failed_task_ids: Vec<_> = all_failed_tasks
            .iter()
            .map(|task| task.task_context().task_id)
            .collect();
        failed_task_ids.sort();
        assert_eq!(failed_task_ids, vec![1, 2, 3]);

        // Verify worker state: Workers 1 and 3 should be dead, worker 2 should be alive
        let worker_snapshots = worker_manager.worker_snapshots()?;
        println!("worker_snapshots: {:?}", worker_snapshots);
        assert_eq!(
            worker_snapshots.len(),
            1,
            "Only worker2 should remain alive"
        );
        assert_eq!(worker_snapshots[0].worker_id(), &worker2_id);

        Ok(())
    }
}
