use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use super::{
    default::DefaultScheduler, linear::LinearScheduler, SchedulableTask, Scheduler, WorkerSnapshot,
};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        dispatcher::{DispatcherActor, DispatcherHandle},
        task::{Task, TaskId},
        worker::{Worker, WorkerManager},
    },
    utils::{
        channel::{create_channel, create_oneshot_channel, OneshotReceiver, Receiver, Sender},
        joinset::JoinSet,
    },
};

#[allow(dead_code)]
struct SchedulerActor<W: Worker, S: Scheduler<W::Task>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    scheduler: S,
}

impl<W: Worker> SchedulerActor<W, DefaultScheduler<W::Task>> {
    fn default_scheduler(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self {
            worker_manager,
            scheduler: DefaultScheduler::new(),
        }
    }
}

#[allow(dead_code)]
impl<W: Worker> SchedulerActor<W, LinearScheduler<W::Task>> {
    fn linear_scheduler(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self {
            worker_manager,
            scheduler: LinearScheduler::new(),
        }
    }
}

impl<W, S> SchedulerActor<W, S>
where
    W: Worker,
    S: Scheduler<W::Task> + Send + 'static,
{
    fn spawn_scheduler_actor(
        mut scheduler: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> SchedulerHandle<W::Task> {
        // Spawn a dispatcher actor to handle task dispatch and await task results
        let initial_worker_snapshots = scheduler
            .worker_manager
            .workers()
            .values()
            .map(WorkerSnapshot::from)
            .collect::<Vec<_>>();
        scheduler
            .scheduler
            .update_worker_state(&initial_worker_snapshots);
        let dispatcher_actor = DispatcherActor::new(scheduler.worker_manager);
        let dispatcher_handle = DispatcherActor::spawn_dispatcher_actor(dispatcher_actor, joinset);

        // Spawn the scheduler actor to schedule tasks and dispatch them to the dispatcher
        let (scheduler_sender, scheduler_receiver) = create_channel(1);
        joinset.spawn(Self::run_scheduler_loop(
            scheduler.scheduler,
            scheduler_receiver,
            dispatcher_handle,
        ));
        SchedulerHandle::new(scheduler_sender)
    }

    #[instrument(name = "FlotillaScheduler", skip_all)]
    async fn run_scheduler_loop(
        mut scheduler: S,
        mut task_rx: Receiver<SchedulableTask<W::Task>>,
        mut dispatcher_handle: DispatcherHandle<W::Task>,
    ) -> DaftResult<()> {
        let mut input_exhausted = false;
        // Keep running until the input is exhausted, i.e. no more new tasks, and there are no more pending tasks in the scheduler
        while !input_exhausted || scheduler.num_pending_tasks() > 0 {
            // 1: Get all tasks that are ready to be scheduled
            let scheduled_tasks = scheduler.get_schedulable_tasks();
            // 2: Dispatch tasks to the dispatcher
            if !scheduled_tasks.is_empty() {
                tracing::debug!("Dispatching tasks: {:?}", scheduled_tasks);

                dispatcher_handle.dispatch_tasks(scheduled_tasks).await?;
            }

            // 3: Concurrently wait for new tasks or worker updates
            tokio::select! {
                maybe_new_task = task_rx.recv() => {
                    // If there are any new tasks, enqueue them all
                    if let Some(new_task) = maybe_new_task {
                        tracing::debug!("Received new task: {:?}", new_task);

                        let mut enqueueable_tasks = vec![new_task];
                        while let Ok(task) = task_rx.try_recv() {
                            enqueueable_tasks.push(task);
                        }
                        scheduler.enqueue_tasks(enqueueable_tasks);
                    } else if !input_exhausted {
                        tracing::debug!("Input exhausted");

                        input_exhausted = true;
                    }
                }
                Some(snapshots) = dispatcher_handle.await_worker_updates() => {
                    tracing::debug!("Received worker updates: {:?}", snapshots);

                    scheduler.update_worker_state(&snapshots);
                }
            }
        }
        tracing::debug!("Scheduler loop complete");
        Ok(())
    }
}

pub(crate) fn spawn_default_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
) -> SchedulerHandle<W::Task> {
    let scheduler = SchedulerActor::default_scheduler(worker_manager);
    SchedulerActor::spawn_scheduler_actor(scheduler, joinset)
}

#[allow(dead_code)]
pub(crate) fn spawn_linear_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
) -> SchedulerHandle<W::Task> {
    let scheduler = SchedulerActor::linear_scheduler(worker_manager);
    SchedulerActor::spawn_scheduler_actor(scheduler, joinset)
}

#[derive(Debug)]
pub(crate) struct SchedulerHandle<T: Task> {
    scheduler_sender: Sender<SchedulableTask<T>>,
}

impl<T: Task> Clone for SchedulerHandle<T> {
    fn clone(&self) -> Self {
        Self {
            scheduler_sender: self.scheduler_sender.clone(),
        }
    }
}

impl<T: Task> SchedulerHandle<T> {
    fn new(scheduler_sender: Sender<SchedulableTask<T>>) -> Self {
        Self { scheduler_sender }
    }

    pub fn prepare_task_for_submission(task: T) -> (SchedulableTask<T>, SubmittedTask) {
        let (result_tx, result_rx) = create_oneshot_channel();
        let cancel_token = CancellationToken::new();

        let task_id = task.task_id().clone();
        let schedulable_task = SchedulableTask::new(task, result_tx, cancel_token.clone());
        let submitted_task = SubmittedTask::new(task_id, result_rx, Some(cancel_token));

        (schedulable_task, submitted_task)
    }

    #[allow(dead_code)]
    pub async fn submit_task(&self, task: T) -> DaftResult<SubmittedTask> {
        let (schedulable_task, submitted_task) = Self::prepare_task_for_submission(task);
        self.scheduler_sender
            .send(schedulable_task)
            .await
            .map_err(|_| {
                DaftError::InternalError("Failed to send task to scheduler".to_string())
            })?;
        Ok(submitted_task)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittedTask {
    _task_id: TaskId,
    result_rx: OneshotReceiver<DaftResult<Vec<MaterializedOutput>>>,
    cancel_token: Option<CancellationToken>,
    finished: bool,
}

impl SubmittedTask {
    fn new(
        task_id: TaskId,
        result_rx: OneshotReceiver<DaftResult<Vec<MaterializedOutput>>>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            _task_id: task_id,
            result_rx,
            cancel_token,
            finished: false,
        }
    }

    #[allow(dead_code)]
    pub fn id(&self) -> &TaskId {
        &self._task_id
    }
}

impl Future for SubmittedTask {
    type Output = DaftResult<Vec<MaterializedOutput>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result_rx.poll_unpin(cx) {
            Poll::Ready(Ok(result)) => {
                self.finished = true;
                Poll::Ready(result)
            }
            // If the receiver is dropped, return no results
            Poll::Ready(Err(_)) => {
                self.finished = true;
                Poll::Ready(Ok(vec![]))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for SubmittedTask {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(cancel_token) = self.cancel_token.take() {
            cancel_token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::scheduling::{
        scheduler::test_utils::setup_workers,
        task::tests::MockTaskFailure,
        tests::{create_mock_partition_ref, MockTask, MockTaskBuilder},
        worker::{tests::MockWorkerManager, WorkerId},
    };

    struct SchedulerActorTestContext {
        scheduler_handle_ref: Arc<SchedulerHandle<MockTask>>,
        joinset: JoinSet<DaftResult<()>>,
    }

    impl SchedulerActorTestContext {
        async fn cleanup(mut self) -> DaftResult<()> {
            drop(self.scheduler_handle_ref);
            while let Some(result) = self.joinset.join_next().await {
                result??;
            }
            Ok(())
        }
    }

    fn setup_scheduler_actor_test_context(
        worker_configs: &[(WorkerId, usize)],
    ) -> SchedulerActorTestContext {
        let workers = setup_workers(worker_configs);
        let worker_manager = Arc::new(MockWorkerManager::new(workers));
        let scheduler = SchedulerActor::default_scheduler(worker_manager);
        let mut joinset = JoinSet::new();
        let scheduler_handle = SchedulerActor::spawn_scheduler_actor(scheduler, &mut joinset);
        SchedulerActorTestContext {
            scheduler_handle_ref: Arc::new(scheduler_handle),
            joinset,
        }
    }

    #[tokio::test]
    async fn test_scheduler_actor_basic_task() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        let partition_ref = create_mock_partition_ref(100, 1024);
        let task = MockTaskBuilder::new(partition_ref.clone()).build();

        let submitted_task = test_context.scheduler_handle_ref.submit_task(task).await?;

        let result = submitted_task.await?;
        assert!(Arc::ptr_eq(&result[0].partition(), &partition_ref));

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_multiple_tasks() -> DaftResult<()> {
        let worker_1: WorkerId = Arc::from("worker1");

        let test_context = setup_scheduler_actor_test_context(&[(worker_1.clone(), 10)]);
        let num_tasks = 1000;
        let task_duration = std::time::Duration::from_millis(100);

        let mut submitted_tasks = Vec::with_capacity(num_tasks);
        for i in 0..num_tasks {
            let task = MockTaskBuilder::new(create_mock_partition_ref(100 + i, 1024 + 1))
                .with_task_id(format!("task-{}", i).into())
                .with_sleep_duration(task_duration)
                .build();
            let submitted_task = test_context
                .scheduler_handle_ref
                .submit_task(task)
                .await
                .unwrap();
            submitted_tasks.push(submitted_task);
        }

        let mut counter = 0;
        for submitted_task in submitted_tasks {
            let result = submitted_task.await?;
            let partition = result[0].partition();
            assert_eq!(partition.num_rows().unwrap(), 100 + counter);
            assert_eq!(partition.size_bytes().unwrap(), Some(1024 + 1));
            counter += 1;
        }
        assert_eq!(counter, num_tasks);

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_multiple_concurrent_tasks() -> DaftResult<()> {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let mut test_context = setup_scheduler_actor_test_context(&[
            (worker_1.clone(), 10),
            (worker_2.clone(), 10),
            (worker_3.clone(), 10),
        ]);
        let num_tasks = 3000;
        let num_concurrent_submitters = 30;
        let num_tasks_per_submitter = num_tasks / num_concurrent_submitters;

        let (submitted_task_tx, mut submitted_task_rx) = create_channel(1);
        for submitter_id in 0..num_concurrent_submitters {
            let scheduler_handle = test_context.scheduler_handle_ref.clone();
            let submitted_task_tx = submitted_task_tx.clone();

            test_context.joinset.spawn(async move {
                for task_id in 0..num_tasks_per_submitter {
                    let num_rows = rand::thread_rng().gen_range(100..1000);
                    let num_bytes = rand::thread_rng().gen_range(1024..1024 * 10);
                    let task_duration =
                        std::time::Duration::from_millis(rand::thread_rng().gen_range(50..150));
                    let task = MockTaskBuilder::new(create_mock_partition_ref(num_rows, num_bytes))
                        .with_task_id(format!("submitter-{}:task-{}", submitter_id, task_id).into())
                        .with_sleep_duration(task_duration)
                        .build();
                    let submitted_task = scheduler_handle.submit_task(task).await.unwrap();
                    submitted_task_tx
                        .send((submitted_task, num_rows, num_bytes))
                        .await
                        .unwrap();
                }
                Ok(())
            });
        }

        drop(submitted_task_tx);
        while let Some((submitted_task, num_rows, num_bytes)) = submitted_task_rx.recv().await {
            let result = submitted_task.await?;
            let partition = result[0].partition();
            assert_eq!(partition.num_rows().unwrap(), num_rows);
            assert_eq!(partition.size_bytes().unwrap(), Some(num_bytes));
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_cancelled_task() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        let partition_ref = create_mock_partition_ref(100, 100);
        let (cancel_notifier, cancel_receiver) = create_oneshot_channel();
        let task = MockTaskBuilder::new(partition_ref.clone())
            .with_cancel_notifier(cancel_notifier)
            .with_sleep_duration(std::time::Duration::from_millis(1000))
            .build();

        let submitted_task = test_context.scheduler_handle_ref.submit_task(task).await?;
        drop(submitted_task);
        cancel_receiver.await.unwrap();

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_multiple_concurrent_tasks_with_cancelled_tasks() -> DaftResult<()>
    {
        let worker_1: WorkerId = Arc::from("worker1");
        let worker_2: WorkerId = Arc::from("worker2");
        let worker_3: WorkerId = Arc::from("worker3");

        let mut test_context = setup_scheduler_actor_test_context(&[
            (worker_1.clone(), 10),
            (worker_2.clone(), 10),
            (worker_3.clone(), 10),
        ]);
        let num_tasks = 3000;
        let num_concurrent_submitters = 30;
        let num_tasks_per_submitter = num_tasks / num_concurrent_submitters;

        let (submitted_task_tx, mut submitted_task_rx) = create_channel(1);
        for submitter_id in 0..num_concurrent_submitters {
            let scheduler_handle = test_context.scheduler_handle_ref.clone();
            let submitted_task_tx = submitted_task_tx.clone();

            test_context.joinset.spawn(async move {
                for task_id in 0..num_tasks_per_submitter {
                    let should_cancel = rand::thread_rng().gen_bool(0.5);

                    let num_rows = rand::thread_rng().gen_range(100..1000);
                    let num_bytes = rand::thread_rng().gen_range(1024..1024 * 10);
                    let mut task =
                        MockTaskBuilder::new(create_mock_partition_ref(num_rows, num_bytes))
                            .with_task_id(
                                format!("submitter-{}:task-{}", submitter_id, task_id).into(),
                            );

                    if should_cancel {
                        let task_duration = std::time::Duration::from_millis(1000);
                        let (cancel_notifier, cancel_receiver) = create_oneshot_channel();
                        task = task
                            .with_cancel_notifier(cancel_notifier)
                            .with_sleep_duration(task_duration);
                        let task = task.build();
                        let submitted_task = scheduler_handle.submit_task(task).await.unwrap();
                        submitted_task_tx
                            .send((submitted_task, num_rows, num_bytes, Some(cancel_receiver)))
                            .await
                            .unwrap();
                    } else {
                        let task_duration =
                            std::time::Duration::from_millis(rand::thread_rng().gen_range(50..150));
                        let task = task.with_sleep_duration(task_duration);
                        let task = task.build();
                        let submitted_task = scheduler_handle.submit_task(task).await.unwrap();
                        submitted_task_tx
                            .send((submitted_task, num_rows, num_bytes, None))
                            .await
                            .unwrap();
                    }
                }
                Ok(())
            });
        }

        drop(submitted_task_tx);
        while let Some((submitted_task, num_rows, num_bytes, maybe_cancel_receiver)) =
            submitted_task_rx.recv().await
        {
            if let Some(cancel_receiver) = maybe_cancel_receiver {
                drop(submitted_task);
                cancel_receiver.await.unwrap();
            } else {
                let result = submitted_task.await?;
                let partition = result[0].partition();
                assert_eq!(partition.num_rows().unwrap(), num_rows);
                assert_eq!(partition.size_bytes().unwrap(), Some(num_bytes));
            }
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_error_from_task() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 100))
            .with_task_id(format!("task-{}", 0).into())
            .with_failure(MockTaskFailure::Error("test error".to_string()))
            .build();
        let submitted_task = test_context.scheduler_handle_ref.submit_task(task).await?;
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
    async fn test_scheduler_actor_panic_from_task() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 100))
            .with_task_id(format!("task-{}", 0).into())
            .with_failure(MockTaskFailure::Panic("test panic".to_string()))
            .build();
        let submitted_task = test_context.scheduler_handle_ref.submit_task(task).await?;
        let result = submitted_task.await?;
        assert_eq!(result.len(), 0);

        let test_context_result = test_context.cleanup().await;
        assert!(test_context_result.is_err());
        assert!(test_context_result
            .unwrap_err()
            .to_string()
            .contains("test panic"));

        Ok(())
    }
}
