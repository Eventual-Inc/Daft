use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;

use super::{
    scheduler::{SchedulableTask, ScheduledTask},
    task::{Task, TaskContext, TaskResult, TaskResultAwaiter, TaskStatus},
    worker::{Worker, WorkerManager},
};
use crate::{
    scheduling::task::TaskResultHandle,
    statistics::{StatisticsEvent, StatisticsManagerRef},
    utils::joinset::JoinSet,
};

pub(super) struct Dispatcher<W: Worker> {
    task_result_futures: JoinSet<TaskResult>,
    running_tasks: HashMap<TaskContext, ScheduledTask<W::Task>>,
}

impl<W: Worker> Dispatcher<W> {
    pub fn new() -> Self {
        Self {
            task_result_futures: JoinSet::new(),
            running_tasks: HashMap::new(),
        }
    }

    pub fn dispatch_tasks(
        &mut self,
        scheduled_tasks: Vec<ScheduledTask<W::Task>>,
        worker_manager: &Arc<dyn WorkerManager<Worker = W>>,
    ) -> DaftResult<()> {
        let mut worker_to_tasks = HashMap::new();

        for scheduled_task in scheduled_tasks {
            let worker_id = scheduled_task.worker_id();
            let task = scheduled_task.task();
            self.running_tasks
                .insert(task.task_context(), scheduled_task);
            worker_to_tasks
                .entry(worker_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        let result_handles = worker_manager.submit_tasks_to_workers(worker_to_tasks)?;
        for result_handle in result_handles {
            let scheduled_task = self
                .running_tasks
                .get(&result_handle.task_context())
                .expect("Task should be present in running_tasks");
            let result_awaiter = TaskResultAwaiter::new(
                result_handle.task_context(),
                result_handle,
                scheduled_task.cancel_token(),
            );
            self.task_result_futures
                .spawn(result_awaiter.await_result());
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
    ) -> DaftResult<Vec<SchedulableTask<W::Task>>> {
        let mut failed_tasks = Vec::new();
        let mut task_results = Vec::new();

        // Wait for at least one task to complete
        if let Some(task_result) = self.task_result_futures.join_next().await {
            task_results.push(task_result?);

            // Collect any additional completed tasks that are immediately available
            while let Some(task_result) = self.task_result_futures.try_join_next() {
                task_results.push(task_result?);
            }

            // Process all completed tasks
            for (task_context, task_status) in task_results {
                let scheduled_task = self
                    .running_tasks
                    .remove(&task_context)
                    .expect("Task should be present in running_tasks");
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
                        failed_tasks.push(schedulable_task);
                    }
                    TaskStatus::WorkerUnavailable => {
                        let schedulable_task = SchedulableTask::new(task, result_tx, canc);
                        failed_tasks.push(schedulable_task);
                    }
                }
            }
        }

        Ok(failed_tasks)
    }

    pub fn has_running_tasks(&self) -> bool {
        !self.task_result_futures.is_empty()
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
        let partition = result[0].partition();
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
            let partition = result[0].partition();
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

        let result = submitted_task.await?;
        assert_eq!(result.len(), 0);

        Ok(())
    }
}
