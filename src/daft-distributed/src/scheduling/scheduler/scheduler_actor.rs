use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use super::{PendingTask, Scheduler, default::DefaultScheduler, linear::LinearScheduler};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        dispatcher::Dispatcher,
        task::{Task, TaskID},
        worker::{Worker, WorkerManager},
    },
    statistics::{StatisticsManagerRef, TaskEvent},
    utils::{
        channel::{
            OneshotReceiver, OneshotSender, UnboundedReceiver, UnboundedSender,
            create_oneshot_channel, create_unbounded_channel,
        },
        joinset::JoinSet,
    },
};

pub(crate) type SchedulerSender<T> = UnboundedSender<PendingTask<T>>;
pub(crate) type SchedulerReceiver<T> = UnboundedReceiver<PendingTask<T>>;

const SCHEDULER_LOG_TARGET: &str = "DaftFlotillaScheduler";
const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

struct SchedulerActor<W: Worker, S: Scheduler<W::Task>> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    scheduler: S,
}

impl<W: Worker> SchedulerActor<W, DefaultScheduler<W::Task>> {
    fn default_scheduler(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self {
            worker_manager,
            scheduler: DefaultScheduler::default(),
        }
    }
}

impl<W: Worker> SchedulerActor<W, LinearScheduler<W::Task>> {
    fn linear_scheduler(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self {
            worker_manager,
            scheduler: LinearScheduler::default(),
        }
    }
}

impl<W, S> SchedulerActor<W, S>
where
    W: Worker,
    S: Scheduler<W::Task> + Send + 'static,
{
    fn spawn_scheduler_actor(
        scheduler: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
        statistics_manager: StatisticsManagerRef,
    ) -> SchedulerHandle<W::Task> {
        tracing::info!(target: SCHEDULER_LOG_TARGET, "Spawning scheduler actor");

        let (scheduler_sender, scheduler_receiver) = create_unbounded_channel();

        // Create dispatcher directly instead of spawning it as an actor
        let dispatcher = Dispatcher::new();

        // Spawn the scheduler actor to schedule tasks and dispatch them directly via the dispatcher
        joinset.spawn(Self::run_scheduler_loop(
            scheduler.scheduler,
            scheduler_receiver,
            dispatcher,
            scheduler.worker_manager,
            statistics_manager,
        ));

        tracing::info!(target: SCHEDULER_LOG_TARGET, "Spawned scheduler actor");
        SchedulerHandle::new(scheduler_sender)
    }

    fn handle_new_tasks(
        maybe_new_task: Option<PendingTask<W::Task>>,
        task_rx: &mut SchedulerReceiver<W::Task>,
        statistics_manager: &StatisticsManagerRef,
        scheduler: &mut S,
        input_exhausted: &mut bool,
    ) -> DaftResult<()> {
        // If there are any new tasks, enqueue them all
        if let Some(new_task) = maybe_new_task {
            let mut enqueueable_tasks = vec![new_task];

            // Drain all available tasks from the channel
            while let Ok(task) = task_rx.try_recv() {
                enqueueable_tasks.push(task);
            }

            tracing::info!(target: SCHEDULER_LOG_TARGET, num_tasks = enqueueable_tasks.len(), "Enqueueing task batch");
            tracing::debug!(target: SCHEDULER_LOG_TARGET, enqueued_tasks = %format!("{:#?}", enqueueable_tasks));

            // Register statistics for all tasks
            for task in &enqueueable_tasks {
                let task_context = task.task_context();
                statistics_manager.handle_event(TaskEvent::Submitted {
                    context: task_context,
                    name: task.task.task_name().clone(),
                })?;
            }

            scheduler.enqueue_tasks(enqueueable_tasks);
        } else if !*input_exhausted {
            tracing::info!(target: SCHEDULER_LOG_TARGET, "Task input stream exhausted");
            *input_exhausted = true;
        }
        Ok(())
    }

    #[instrument(name = "FlotillaScheduler", skip_all)]
    async fn run_scheduler_loop(
        mut scheduler: S,
        mut task_rx: SchedulerReceiver<W::Task>,
        mut dispatcher: Dispatcher<W>,
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        statistics_manager: StatisticsManagerRef,
    ) -> DaftResult<()> {
        let mut input_exhausted = false;
        let mut tick_interval = tokio::time::interval(SCHEDULER_TICK_INTERVAL);
        // Keep running until the input is exhausted, i.e. no more new tasks, and there are no more pending tasks in the scheduler
        while !input_exhausted
            || scheduler.num_pending_tasks() > 0
            || dispatcher.has_running_tasks()
        {
            // Update worker snapshots at the start of each loop iteration
            let worker_snapshots = worker_manager.worker_snapshots()?;
            tracing::info!(target: SCHEDULER_LOG_TARGET,
                num_workers = worker_snapshots.len(),
                pending_tasks = scheduler.num_pending_tasks(),
                "Received worker snapshots");
            tracing::debug!(target: SCHEDULER_LOG_TARGET, worker_snapshots = %format!("{:#?}", worker_snapshots));

            scheduler.update_worker_state(&worker_snapshots);

            // 1: Get all tasks that are ready to be scheduled
            let scheduled_tasks = scheduler.schedule_tasks();
            // 2: Dispatch tasks directly to the dispatcher
            if !scheduled_tasks.is_empty() {
                tracing::info!(target: SCHEDULER_LOG_TARGET, num_tasks = scheduled_tasks.len(), "Scheduling tasks for dispatch");
                tracing::debug!(target: SCHEDULER_LOG_TARGET, scheduled_tasks = %format!("{:#?}", scheduled_tasks));

                // Report to statistics manager
                for task in &scheduled_tasks {
                    let task_context = task.task().task_context();
                    statistics_manager.handle_event(TaskEvent::Scheduled {
                        context: task_context,
                    })?;
                }

                dispatcher.dispatch_tasks(scheduled_tasks, &worker_manager)?;
            }

            // 3: Send autoscaling request if needed
            let autoscaling_request = scheduler.get_autoscaling_request();
            if let Some(request) = autoscaling_request {
                tracing::info!(target: SCHEDULER_LOG_TARGET, autoscaling_request = %format!("{:#?}", request), "Sending autoscaling request");
                worker_manager.try_autoscale(request)?;
            }

            // 4: Concurrently wait for new tasks, task completions, or periodic tick
            tokio::select! {
                maybe_new_task = task_rx.recv(), if !input_exhausted => {
                    Self::handle_new_tasks(maybe_new_task, &mut task_rx, &statistics_manager, &mut scheduler, &mut input_exhausted)?;
                }
                failed_tasks = dispatcher.await_completed_tasks(&worker_manager, &statistics_manager), if dispatcher.has_running_tasks() => {
                    let failed_tasks = failed_tasks?;
                    // Re-enqueue any failed tasks
                    if !failed_tasks.is_empty() {
                        scheduler.enqueue_tasks(failed_tasks);
                    }
                }
                _ = tick_interval.tick() => {
                    // Tick completed - worker snapshots will be updated at the top of the next loop iteration
                }
            }
        }

        tracing::info!(target: SCHEDULER_LOG_TARGET, "Scheduler event loop completed");
        Ok(())
    }
}

pub(crate) fn spawn_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task> {
    // Check for environment variable to use linear scheduler
    if std::env::var("DAFT_SCHEDULER_LINEAR")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        spawn_linear_scheduler_actor(worker_manager, joinset, statistics_manager)
    } else {
        spawn_default_scheduler_actor(worker_manager, joinset, statistics_manager)
    }
}

fn spawn_default_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task> {
    tracing::info!(target: SCHEDULER_LOG_TARGET, "Spawning default scheduler actor");

    let scheduler = SchedulerActor::default_scheduler(worker_manager);
    SchedulerActor::spawn_scheduler_actor(scheduler, joinset, statistics_manager)
}

fn spawn_linear_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task> {
    tracing::info!(target: SCHEDULER_LOG_TARGET, "Creating linear scheduler");

    let scheduler = SchedulerActor::linear_scheduler(worker_manager);
    SchedulerActor::spawn_scheduler_actor(scheduler, joinset, statistics_manager)
}

#[derive(Debug)]
pub(crate) struct SchedulerHandle<T: Task> {
    scheduler_sender: SchedulerSender<T>,
}

impl<T: Task> Clone for SchedulerHandle<T> {
    fn clone(&self) -> Self {
        Self {
            scheduler_sender: self.scheduler_sender.clone(),
        }
    }
}

impl<T: Task> SchedulerHandle<T> {
    fn new(scheduler_sender: SchedulerSender<T>) -> Self {
        Self { scheduler_sender }
    }

    pub fn prepare_task_for_submission(
        submittable_task: SubmittableTask<T>,
    ) -> (PendingTask<T>, SubmittedTask) {
        let task_id = submittable_task.task.task_id();
        let (result_tx, result_rx) = create_oneshot_channel();
        let schedulable_task = PendingTask::new(
            submittable_task.task,
            result_tx,
            submittable_task.cancel_token.clone(),
        );
        let submitted_task = SubmittedTask::new(
            task_id,
            result_rx,
            Some(submittable_task.cancel_token),
            submittable_task.notify_tokens,
        );

        (schedulable_task, submitted_task)
    }

    fn submit_task(&self, submittable_task: SubmittableTask<T>) -> DaftResult<SubmittedTask> {
        let (schedulable_task, submitted_task) =
            Self::prepare_task_for_submission(submittable_task);
        self.scheduler_sender.send(schedulable_task).map_err(|_| {
            DaftError::InternalError("Failed to send task to scheduler".to_string())
        })?;
        Ok(submitted_task)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittableTask<T: Task> {
    task: T,
    cancel_token: CancellationToken,
    notify_tokens: Vec<OneshotSender<()>>,
}

impl<T: Task> SubmittableTask<T> {
    pub fn new(task: T) -> Self {
        let cancel_token = CancellationToken::new();
        Self {
            task,
            cancel_token,
            notify_tokens: vec![],
        }
    }

    pub fn task(&self) -> &T {
        &self.task
    }

    pub fn add_notify_token(mut self) -> (Self, OneshotReceiver<()>) {
        let (notify_token, notify_rx) = create_oneshot_channel();
        self.notify_tokens.push(notify_token);
        (self, notify_rx)
    }

    pub fn with_new_task(mut self, new_task: T) -> Self {
        self.task = new_task;
        self
    }

    pub fn submit(self, scheduler_handle: &SchedulerHandle<T>) -> DaftResult<SubmittedTask> {
        scheduler_handle.submit_task(self)
    }
}

#[derive(Debug)]
pub(crate) struct SubmittedTask {
    _task_id: TaskID,
    result_rx: OneshotReceiver<DaftResult<Option<MaterializedOutput>>>,
    cancel_token: Option<CancellationToken>,
    notify_tokens: Vec<OneshotSender<()>>,
    finished: bool,
}

impl SubmittedTask {
    fn new(
        task_id: TaskID,
        result_rx: OneshotReceiver<DaftResult<Option<MaterializedOutput>>>,
        cancel_token: Option<CancellationToken>,
        notify_tokens: Vec<OneshotSender<()>>,
    ) -> Self {
        Self {
            _task_id: task_id,
            result_rx,
            cancel_token,
            notify_tokens,
            finished: false,
        }
    }

    #[allow(dead_code)]
    pub fn id(&self) -> &TaskID {
        &self._task_id
    }
}

impl Future for SubmittedTask {
    type Output = DaftResult<Option<MaterializedOutput>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result_rx.poll_unpin(cx) {
            Poll::Ready(Ok(result)) => {
                self.finished = true;
                for notify_token in self.notify_tokens.drain(..) {
                    let _ = notify_token.send(());
                }
                Poll::Ready(result)
            }
            // If the sender is dropped (i.e. the task is cancelled), return no results
            Poll::Ready(Err(_)) => {
                self.finished = true;
                for notify_token in self.notify_tokens.drain(..) {
                    let _ = notify_token.send(());
                }
                Poll::Ready(Ok(None))
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
    use crate::{
        scheduling::{
            scheduler::test_utils::setup_workers,
            task::tests::MockTaskFailure,
            tests::{MockTask, MockTaskBuilder, create_mock_partition_ref},
            worker::{WorkerId, tests::MockWorkerManager},
        },
        utils::channel::create_channel,
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
        let scheduler_handle = SchedulerActor::spawn_scheduler_actor(
            scheduler,
            &mut joinset,
            StatisticsManagerRef::default(),
        );
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

        let submittable_task = SubmittableTask::new(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;

        let result = submitted_task.await?;
        assert!(Arc::ptr_eq(
            &result.unwrap().partitions()[0],
            &partition_ref
        ));

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
                .with_task_id(i as u32)
                .with_sleep_duration(task_duration)
                .build();
            let submittable_task = SubmittableTask::new(task);
            let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
            submitted_tasks.push(submitted_task);
        }

        let mut counter = 0;
        for submitted_task in submitted_tasks {
            let result = submitted_task.await?;
            let partition = result.unwrap().partitions()[0].clone();
            assert_eq!(partition.num_rows(), 100 + counter);
            assert_eq!(partition.size_bytes(), 1024 + 1);
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
                        .with_task_id(submitter_id * num_tasks_per_submitter + task_id)
                        .with_sleep_duration(task_duration)
                        .build();
                    let submittable_task = SubmittableTask::new(task);
                    let submitted_task = submittable_task.submit(&scheduler_handle)?;
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
            let partition = result.unwrap().partitions()[0].clone();
            assert_eq!(partition.num_rows(), num_rows);
            assert_eq!(partition.size_bytes(), num_bytes);
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

        let submittable_task = SubmittableTask::new(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
        drop(submitted_task);
        cancel_receiver.await.unwrap();

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_multiple_concurrent_tasks_with_cancelled_tasks() -> DaftResult<()>
    {
        let num_workers = 30;
        let mut test_context = setup_scheduler_actor_test_context(
            &(0..num_workers)
                .map(|i| (format!("worker{}", i).into(), 1))
                .collect::<Vec<_>>(),
        );
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
                            .with_task_id(submitter_id * num_tasks_per_submitter + task_id);

                    if should_cancel {
                        let task_duration = std::time::Duration::from_millis(1000);
                        let (cancel_notifier, cancel_receiver) = create_oneshot_channel();
                        task = task
                            .with_cancel_notifier(cancel_notifier)
                            .with_sleep_duration(task_duration);
                        let task = task.build();
                        let submittable_task = SubmittableTask::new(task);
                        let submitted_task = submittable_task.submit(&scheduler_handle)?;
                        submitted_task_tx
                            .send((submitted_task, num_rows, num_bytes, Some(cancel_receiver)))
                            .await
                            .unwrap();
                    } else {
                        let task_duration =
                            std::time::Duration::from_millis(rand::thread_rng().gen_range(50..150));
                        let task = task.with_sleep_duration(task_duration);
                        let task = task.build();
                        let submittable_task = SubmittableTask::new(task);
                        let submitted_task = submittable_task.submit(&scheduler_handle)?;
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
                let partition = result.unwrap().partitions()[0].clone();
                assert_eq!(partition.num_rows(), num_rows);
                assert_eq!(partition.size_bytes(), num_bytes);
            }
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_error_from_task() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 100))
            .with_task_id(0)
            .with_failure(MockTaskFailure::Error("test error".to_string()))
            .build();
        let submittable_task = SubmittableTask::new(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
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
            .with_task_id(0)
            .with_failure(MockTaskFailure::Panic("test panic".to_string()))
            .build();
        let submittable_task = SubmittableTask::new(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
        let result = submitted_task.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test panic"));

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_with_no_workers_can_autoscale() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[]);

        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 100))
            .with_task_id(0)
            .build();
        let submittable_task = SubmittableTask::new(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
        let result = submitted_task.await?;
        assert_eq!(result.unwrap().partitions().len(), 1);

        test_context.cleanup().await?;
        Ok(())
    }
}
