use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use common_error::{DaftError, DaftResult};
use common_runtime::JoinSet;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use super::{PendingTask, Scheduler, default::DefaultScheduler, linear::LinearScheduler};
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        dispatcher::Dispatcher,
        task::{Task, TaskID},
        worker::{AutoscaleDemandId, Worker, WorkerManager},
    },
    statistics::{StatisticsManagerRef, TaskEvent},
    utils::channel::{
        OneshotReceiver, OneshotSender, UnboundedReceiver, UnboundedSender, create_oneshot_channel,
        create_unbounded_channel,
    },
};

pub(crate) type SchedulerSender<T> = UnboundedSender<PendingTask<T>>;
pub(crate) type SchedulerReceiver<T> = UnboundedReceiver<PendingTask<T>>;

const SCHEDULER_LOG_TARGET: &str = "DaftFlotillaScheduler";
const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Owned state for the scheduler event loop — one instance per plan run.
/// Runs until `task_rx` closes and both scheduler + dispatcher are drained.
struct SchedulerLoop<W: Worker, S: Scheduler<W::Task>> {
    scheduler: S,
    task_rx: SchedulerReceiver<W::Task>,
    dispatcher: Dispatcher<W>,
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    statistics_manager: StatisticsManagerRef,
    input_exhausted: bool,
    /// Identifies this loop's slice of the cluster-wide autoscaling demand.
    /// Ray's `request_resources()` is a single replace-on-write slot, so the worker
    /// manager keys demand by owner and always publishes the union; without this,
    /// concurrent plans would overwrite each other's requests.
    autoscale_demand_id: AutoscaleDemandId,
}

impl<W, S> SchedulerLoop<W, S>
where
    W: Worker,
    S: Scheduler<W::Task> + Send + 'static,
{
    fn new(
        scheduler: S,
        task_rx: SchedulerReceiver<W::Task>,
        worker_manager: Arc<dyn WorkerManager<Worker = W>>,
        statistics_manager: StatisticsManagerRef,
    ) -> Self {
        let dispatcher = Dispatcher::new(statistics_manager.clone());
        Self {
            scheduler,
            task_rx,
            dispatcher,
            worker_manager,
            statistics_manager,
            input_exhausted: false,
            autoscale_demand_id: AutoscaleDemandId::new(),
        }
    }

    fn handle_new_tasks(&mut self, maybe_new_task: Option<PendingTask<W::Task>>) -> DaftResult<()> {
        if let Some(new_task) = maybe_new_task {
            let mut enqueueable_tasks = vec![new_task];

            // Drain all available tasks from the channel
            while let Ok(task) = self.task_rx.try_recv() {
                enqueueable_tasks.push(task);
            }

            tracing::info!(target: SCHEDULER_LOG_TARGET, num_tasks = enqueueable_tasks.len(), "Enqueueing task batch");
            tracing::debug!(target: SCHEDULER_LOG_TARGET, enqueued_tasks = %format!("{:#?}", enqueueable_tasks));

            for task in &enqueueable_tasks {
                self.statistics_manager.handle_event(TaskEvent::Submitted {
                    context: task.task_context(),
                    name: task.task.task_name().clone(),
                    // TODO(perf): Avoid building TaskMetadata unless a subscriber/export path needs it.
                    // This currently clones scan paths and estimates scan sizes for every submitted task,
                    // even when task lifecycle event emission is disabled. The right fix likely belongs
                    // in the task-event wiring layer rather than StatisticsSubscriber.
                    metadata: task.task_metadata(),
                })?;
            }

            self.scheduler.enqueue_tasks(enqueueable_tasks);
        } else if !self.input_exhausted {
            tracing::info!(target: SCHEDULER_LOG_TARGET, "Task input stream exhausted");
            self.input_exhausted = true;
        }
        Ok(())
    }

    #[instrument(name = "FlotillaScheduler", skip_all)]
    async fn run(mut self) -> DaftResult<()> {
        self.event_loop().await?;
        tracing::info!(target: SCHEDULER_LOG_TARGET, "Scheduler event loop completed");
        Ok(())
    }

    fn schedule_and_dispatch(&mut self) -> DaftResult<()> {
        let _dispatch_guard = self.worker_manager.begin_dispatch();
        let worker_snapshots = self.worker_manager.worker_snapshots()?;
        tracing::info!(
            target: SCHEDULER_LOG_TARGET,
            num_workers = worker_snapshots.len(),
            pending_tasks = self.scheduler.num_pending_tasks(),
            "Received worker snapshots"
        );
        tracing::debug!(
            target: SCHEDULER_LOG_TARGET,
            worker_snapshots = %format!("{:#?}", worker_snapshots)
        );

        self.scheduler.update_worker_state(&worker_snapshots);

        // 1: Send autoscaling request if needed (scale up).
        // We do this before scheduling tasks to ensure that the autoscaler sees the true demand
        // and not just the residual demand after scheduling.
        let autoscaling_request = self.scheduler.get_autoscaling_request();
        if let Some(request) = autoscaling_request {
            tracing::info!(
                target: SCHEDULER_LOG_TARGET,
                autoscaling_request = %format!("{:#?}", request),
                "Sending autoscaling request"
            );
            self.worker_manager
                .try_autoscale(self.autoscale_demand_id, request)?;
        }

        // 2: Get all tasks that are ready to be scheduled
        let (scheduled_tasks, cancelled_tasks) = self.scheduler.schedule_tasks();
        for task in &cancelled_tasks {
            self.statistics_manager.handle_event(TaskEvent::Cancelled {
                context: task.task_context(),
            })?;
        }
        // 3: Dispatch tasks directly to the dispatcher
        if !scheduled_tasks.is_empty() {
            tracing::info!(
                target: SCHEDULER_LOG_TARGET,
                num_tasks = scheduled_tasks.len(),
                "Scheduling tasks for dispatch"
            );
            tracing::debug!(
                target: SCHEDULER_LOG_TARGET,
                scheduled_tasks = %format!("{:#?}", scheduled_tasks)
            );

            for task in &scheduled_tasks {
                self.statistics_manager.handle_event(TaskEvent::Scheduled {
                    context: task.task().task_context(),
                    worker_id: task.worker_id(),
                })?;
            }

            self.dispatcher
                .dispatch_tasks(scheduled_tasks, &self.worker_manager)?;
        }
        Ok(())
    }

    async fn event_loop(&mut self) -> DaftResult<()> {
        let mut tick_interval = tokio::time::interval(SCHEDULER_TICK_INTERVAL);

        while !self.input_exhausted
            || self.scheduler.num_pending_tasks() > 0
            || self.dispatcher.has_running_tasks()
        {
            self.schedule_and_dispatch()?;

            // 4: Concurrently wait for new tasks, task completions, or periodic tick.
            let Self {
                task_rx,
                dispatcher,
                worker_manager,
                input_exhausted,
                ..
            } = &mut *self;
            let worker_manager: &Arc<dyn WorkerManager<Worker = W>> = worker_manager;
            let select_result = tokio::select! {
                maybe_new_task = task_rx.recv(), if !*input_exhausted => {
                    SelectOutcome::NewTask(maybe_new_task)
                }
                failed_tasks = dispatcher.await_completed_tasks(worker_manager), if dispatcher.has_running_tasks() => {
                    SelectOutcome::CompletedTasks(failed_tasks?)
                }
                _ = tick_interval.tick() => SelectOutcome::Tick,
            };

            match select_result {
                SelectOutcome::NewTask(maybe_new_task) => {
                    self.handle_new_tasks(maybe_new_task)?;
                }
                SelectOutcome::CompletedTasks(failed_tasks) => {
                    if !failed_tasks.is_empty() {
                        self.scheduler.enqueue_tasks(failed_tasks);
                    }
                }
                SelectOutcome::Tick => {
                    // Worker snapshots refreshed at top of next iteration.
                }
            }
        }

        Ok(())
    }
}

impl<W: Worker, S: Scheduler<W::Task>> Drop for SchedulerLoop<W, S> {
    fn drop(&mut self) {
        // Aborting the owning JoinSet drops the future without returning from event_loop.
        if let Err(e) = self
            .worker_manager
            .clear_autoscale_demand(self.autoscale_demand_id)
        {
            tracing::warn!(
                target: SCHEDULER_LOG_TARGET,
                error = %e,
                "Failed to clear autoscaling demand on scheduler shutdown"
            );
        }
    }
}

enum SelectOutcome<T: Task> {
    NewTask(Option<PendingTask<T>>),
    CompletedTasks(Vec<PendingTask<T>>),
    Tick,
}

pub(crate) fn spawn_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task> {
    if std::env::var("DAFT_SCHEDULER_LINEAR")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        spawn_linear_scheduler_actor(worker_manager, joinset, statistics_manager)
    } else {
        spawn_default_scheduler_actor(worker_manager, joinset, statistics_manager)
    }
}

fn spawn_scheduler_loop<W, S>(
    scheduler: S,
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task>
where
    W: Worker,
    S: Scheduler<W::Task> + Send + 'static,
{
    let (scheduler_sender, scheduler_receiver) = create_unbounded_channel();
    let loop_state = SchedulerLoop::new(
        scheduler,
        scheduler_receiver,
        worker_manager,
        statistics_manager,
    );
    joinset.spawn(loop_state.run());
    SchedulerHandle::new(scheduler_sender)
}

fn spawn_default_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task> {
    tracing::info!(target: SCHEDULER_LOG_TARGET, "Spawning default scheduler actor");
    spawn_scheduler_loop(
        DefaultScheduler::<W::Task>::default(),
        worker_manager,
        joinset,
        statistics_manager,
    )
}

fn spawn_linear_scheduler_actor<W: Worker>(
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
    joinset: &mut JoinSet<DaftResult<()>>,
    statistics_manager: StatisticsManagerRef,
) -> SchedulerHandle<W::Task> {
    tracing::info!(target: SCHEDULER_LOG_TARGET, "Spawning linear scheduler actor");
    spawn_scheduler_loop(
        LinearScheduler::<W::Task>::default(),
        worker_manager,
        joinset,
        statistics_manager,
    )
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
    notify_tokens: Vec<OneshotSender<TaskID>>,
}

impl<T: Task> SubmittableTask<T> {
    pub fn new(
        task: T,
        cancel_token: CancellationToken,
        notify_tokens: Vec<OneshotSender<TaskID>>,
    ) -> Self {
        Self {
            task,
            cancel_token,
            notify_tokens,
        }
    }

    #[cfg(test)]
    pub fn task_only(task: T) -> Self {
        let cancel_token = CancellationToken::new();
        Self {
            task,
            cancel_token,
            notify_tokens: vec![],
        }
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
    notify_tokens: Vec<OneshotSender<TaskID>>,
    finished: bool,
}

impl SubmittedTask {
    fn new(
        task_id: TaskID,
        result_rx: OneshotReceiver<DaftResult<Option<MaterializedOutput>>>,
        cancel_token: Option<CancellationToken>,
        notify_tokens: Vec<OneshotSender<TaskID>>,
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
                let task_id = self._task_id;
                for notify_token in self.notify_tokens.drain(..) {
                    let _ = notify_token.send(task_id);
                }
                Poll::Ready(result)
            }
            // If the sender is dropped (i.e. the task is cancelled), return no results
            Poll::Ready(Err(_)) => {
                self.finished = true;
                let task_id = self._task_id;
                for notify_token in self.notify_tokens.drain(..) {
                    let _ = notify_token.send(task_id);
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
    use std::{
        collections::HashSet,
        sync::{Barrier, Mutex, TryLockError},
    };

    use rand::Rng;

    use super::*;
    use crate::{
        scheduling::{
            scheduler::test_utils::setup_workers,
            task::tests::MockTaskFailure,
            tests::{MockTask, MockTaskBuilder, create_mock_partition_ref},
            worker::{
                WorkerId,
                tests::{MockWorker, MockWorkerManager},
            },
        },
        utils::channel::create_channel,
    };

    struct SchedulerActorTestContext {
        scheduler_handle_ref: Arc<SchedulerHandle<MockTask>>,
        worker_manager: Arc<MockWorkerManager>,
        joinset: JoinSet<DaftResult<()>>,
        autoscale_demand_id: AutoscaleDemandId,
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
        let mut joinset = JoinSet::new();

        let (scheduler_sender, scheduler_receiver) = create_unbounded_channel();
        let loop_state = SchedulerLoop::new(
            DefaultScheduler::<MockTask>::default(),
            scheduler_receiver,
            worker_manager.clone(),
            StatisticsManagerRef::default(),
        );
        let autoscale_demand_id = loop_state.autoscale_demand_id;
        joinset.spawn(loop_state.run());
        let scheduler_handle = SchedulerHandle::new(scheduler_sender);

        SchedulerActorTestContext {
            scheduler_handle_ref: Arc::new(scheduler_handle),
            worker_manager,
            joinset,
            autoscale_demand_id,
        }
    }

    /// Retirement of idle workers is now owned entirely by the worker manager's own
    /// background reaper, not the scheduler. This test verifies the scheduler no longer
    /// drives retirement on normal ticks — it never touches the worker pool for downscale
    /// purposes, so there is a single retirement authority (see #5683).
    #[tokio::test]
    async fn test_scheduler_actor_does_not_retire_during_ticks() -> DaftResult<()> {
        let ctx = setup_scheduler_actor_test_context(&[
            (Arc::from("worker1"), 1),
            (Arc::from("worker2"), 1),
        ]);

        tokio::time::sleep(Duration::from_millis(20)).await;
        // The scheduler must not clear demand mid-query; that only happens on completion.
        assert_eq!(ctx.worker_manager.clear_demand_call_count(), 0);

        ctx.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_clears_demand_on_shutdown() -> DaftResult<()> {
        let ctx = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        // Drop the scheduler handle so the loop drains and exits, triggering the demand clear.
        drop(ctx.scheduler_handle_ref);
        let mut joinset = ctx.joinset;
        while let Some(result) = joinset.join_next().await {
            result??;
        }
        // On completion the scheduler clears outstanding autoscaling demand exactly once,
        // and does not retire workers (that is the reaper's job).
        assert_eq!(ctx.worker_manager.clear_demand_call_count(), 1);
        // ...and it retracts its own demand slice, not the cluster-wide request.
        assert_eq!(
            ctx.worker_manager.cleared_demand_ids(),
            vec![ctx.autoscale_demand_id]
        );
        Ok(())
    }

    /// Ray's `request_resources()` is a single cluster-wide slot, so autoscaling demand
    /// must be attributed per plan: two concurrently running scheduler loops each own a
    /// slice, and finishing one must only retract that one. Otherwise a finishing query
    /// cancels the capacity a still-running query is waiting on.
    #[tokio::test]
    async fn test_concurrent_scheduler_loops_own_separate_autoscale_demand() -> DaftResult<()> {
        // Start with an empty pool so the loops are pushed onto the autoscaling path.
        let worker_manager = Arc::new(MockWorkerManager::new(setup_workers(&[])));
        let mut joinset = JoinSet::new();
        let mut handles = Vec::new();
        let mut demand_ids = Vec::new();

        for _ in 0..2 {
            let (scheduler_sender, scheduler_receiver) = create_unbounded_channel();
            let loop_state = SchedulerLoop::new(
                DefaultScheduler::<MockTask>::default(),
                scheduler_receiver,
                worker_manager.clone(),
                StatisticsManagerRef::default(),
            );
            demand_ids.push(loop_state.autoscale_demand_id);
            joinset.spawn(loop_state.run());
            handles.push(SchedulerHandle::new(scheduler_sender));
        }

        assert_ne!(
            demand_ids[0], demand_ids[1],
            "concurrent scheduler loops must not share an autoscaling demand id"
        );

        for handle in &handles {
            let task = MockTaskBuilder::new(create_mock_partition_ref(100, 100)).build();
            let submitted = SubmittableTask::task_only(task).submit(handle)?;
            assert_eq!(submitted.await?.unwrap().partitions().len(), 1);
        }

        drop(handles);
        while let Some(result) = joinset.join_next().await {
            result??;
        }

        // Whether a given loop had to ask for capacity is timing-dependent (the mock
        // manager materializes workers into a shared pool), but every request that is
        // made must be attributed to the loop that made it.
        let owners = demand_ids.iter().copied().collect::<HashSet<_>>();
        for requested in worker_manager.autoscale_demand_ids() {
            assert!(
                owners.contains(&requested),
                "autoscaling request published under an unknown owner"
            );
        }

        // Both loops finished, so both slices — and only those — are retracted.
        let cleared = worker_manager
            .cleared_demand_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(cleared, owners);
        Ok(())
    }

    /// A failed query is exactly the case where previously signaled autoscaling demand no
    /// longer has work behind it, and Ray's request_resources() is sticky — so the
    /// scheduler must clear demand on the error-exit path too, not just on clean shutdown.
    #[tokio::test]
    async fn test_scheduler_actor_clears_demand_on_error_exit() -> DaftResult<()> {
        let ctx = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        // Force the next scheduler iteration to fail at the top of the loop.
        ctx.worker_manager.set_fail_worker_snapshots(true);

        // Submit a task so the loop keeps iterating (and hits the injected failure).
        let task = MockTaskBuilder::new(create_mock_partition_ref(100, 100)).build();
        let submittable_task = SubmittableTask::task_only(task);
        let _submitted_task = submittable_task.submit(&ctx.scheduler_handle_ref)?;

        drop(ctx.scheduler_handle_ref);
        let mut joinset = ctx.joinset;
        let mut saw_error = false;
        while let Some(result) = joinset.join_next().await {
            if result?.is_err() {
                saw_error = true;
            }
        }
        assert!(saw_error, "scheduler loop should have exited with an error");
        // Demand must be cleared exactly once even though the loop exited with an error.
        assert_eq!(ctx.worker_manager.clear_demand_call_count(), 1);
        Ok(())
    }

    fn unspawned_scheduler_loop(
        worker_manager: Arc<MockWorkerManager>,
    ) -> (
        SchedulerLoop<MockWorker, DefaultScheduler<MockTask>>,
        SchedulerHandle<MockTask>,
    ) {
        let (sender, receiver) = create_unbounded_channel();
        (
            SchedulerLoop::new(
                DefaultScheduler::with_autoscaling_threshold(1.25),
                receiver,
                worker_manager,
                StatisticsManagerRef::default(),
            ),
            SchedulerHandle::new(sender),
        )
    }

    fn enqueue_mock_task(
        loop_state: &mut SchedulerLoop<MockWorker, DefaultScheduler<MockTask>>,
        task_id: TaskID,
    ) -> DaftResult<SubmittedTask> {
        let task = MockTaskBuilder::default().with_task_id(task_id).build();
        let (pending, submitted) =
            SchedulerHandle::prepare_task_for_submission(SubmittableTask::task_only(task));
        loop_state.handle_new_tasks(Some(pending))?;
        Ok(submitted)
    }

    #[tokio::test]
    async fn test_scheduler_abort_clears_only_its_published_demand() -> DaftResult<()> {
        let manager = Arc::new(MockWorkerManager::new(setup_workers(&[])));
        manager.enable_dispatch_gate_checks();
        manager.set_autoscale_creates_workers(false);
        let (mut aborted, aborted_handle) = unspawned_scheduler_loop(manager.clone());
        let (mut live, live_handle) = unspawned_scheduler_loop(manager.clone());
        let aborted_id = aborted.autoscale_demand_id;
        let live_id = live.autoscale_demand_id;
        assert_ne!(aborted_id, live_id);
        let aborted_task = enqueue_mock_task(&mut aborted, 0)?;
        let live_task = enqueue_mock_task(&mut live, 1)?;

        let mut aborted = Box::pin(aborted.run());
        let mut live = Box::pin(live.run());
        // Poll through publication to the scheduler's await without any timer sleeps.
        // No workers are materialized, so both owners still have unsatisfied demand.
        assert!(futures::poll!(aborted.as_mut()).is_pending());
        assert!(futures::poll!(live.as_mut()).is_pending());
        assert_eq!(
            manager.active_demand_ids(),
            HashSet::from([aborted_id, live_id])
        );
        assert_eq!(manager.clear_demand_call_count(), 0);
        assert!(manager.dispatch_gate.try_lock().is_ok());

        // Abort a future that has already published, not one that was never polled.
        // Keep its input handle alive so channel closure cannot explain cleanup.
        let mut joinset = JoinSet::new();
        joinset.spawn(aborted);
        joinset.abort_all();
        let mut aborted_count = 0;
        while let Some(result) = joinset.join_next().await {
            assert!(result.is_err(), "the scheduler should have been aborted");
            aborted_count += 1;
        }
        assert_eq!(aborted_count, 1);
        assert_eq!(manager.clear_demand_call_count(), 1);
        assert_eq!(manager.cleared_demand_ids(), vec![aborted_id]);
        assert_eq!(manager.active_demand_ids(), HashSet::from([live_id]));
        assert!(manager.dispatch_gate.try_lock().is_ok());
        assert!(aborted_task.await?.is_none());
        drop(aborted_handle);

        // Let the other owner finish normally. An input task wakes it immediately,
        // avoiding dependence on the scheduler's next periodic tick.
        manager.set_autoscale_creates_workers(true);
        let wake_task =
            SubmittableTask::task_only(MockTaskBuilder::default().with_task_id(2).build())
                .submit(&live_handle)?;
        drop(live_handle);
        live.await?;
        assert!(live_task.await?.is_some());
        assert!(wake_task.await?.is_some());
        assert_eq!(manager.cleared_demand_ids(), vec![aborted_id, live_id]);
        assert_eq!(manager.clear_demand_call_count(), 2);
        assert!(manager.active_demand_ids().is_empty());
        assert!(manager.dispatch_gate.try_lock().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_releases_dispatch_gate_while_awaiting() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let manager = Arc::new(MockWorkerManager::new(setup_workers(&[(
            worker_id.clone(),
            1,
        )])));
        manager.enable_dispatch_gate_checks();
        let (mut loop_state, handle) = unspawned_scheduler_loop(manager.clone());
        let demand_id = loop_state.autoscale_demand_id;
        let submitted = enqueue_mock_task(&mut loop_state, 0)?;
        let mut running = Box::pin(loop_state.run());

        // On this current-thread runtime the dispatched result future has not run
        // yet: the scheduler is awaiting input/completion with an active worker.
        assert!(futures::poll!(running.as_mut()).is_pending());
        assert!(manager.dispatch_gate.try_lock().is_ok());
        assert!(!manager.try_reap_idle_worker(&worker_id));
        assert_eq!(manager.clear_demand_call_count(), 0);

        drop(handle);
        running.await?;
        assert!(submitted.await?.is_some());
        assert_eq!(manager.cleared_demand_ids(), vec![demand_id]);
        assert!(manager.dispatch_gate.try_lock().is_ok());
        Ok(())
    }

    async fn assert_dispatch_error_releases_gate(
        worker_configs: &[(WorkerId, usize)],
        inject_failure: fn(&MockWorkerManager, bool),
        expected_error: &str,
    ) -> DaftResult<()> {
        let manager = Arc::new(MockWorkerManager::new(setup_workers(worker_configs)));
        manager.enable_dispatch_gate_checks();
        inject_failure(&manager, true);
        manager.set_fail_clear_demand(true);
        let (mut loop_state, _handle) = unspawned_scheduler_loop(manager.clone());
        let demand_id = loop_state.autoscale_demand_id;
        let _submitted = enqueue_mock_task(&mut loop_state, 0)?;

        let error = loop_state.run().await.unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("DaftError::InternalError {expected_error}")
        );
        assert!(manager.dispatch_gate.try_lock().is_ok());
        // The cleanup hook also checks the gate is free before injecting its own
        // error; it must neither replace the dispatch error nor be called twice.
        assert_eq!(manager.clear_demand_call_count(), 1);
        assert_eq!(manager.cleared_demand_ids(), vec![demand_id]);
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_error_releases_gate_and_preserves_original_error() -> DaftResult<()> {
        assert_dispatch_error_releases_gate(
            &[],
            MockWorkerManager::set_fail_worker_snapshots,
            "injected worker_snapshots failure",
        )
        .await
    }

    #[tokio::test]
    async fn test_autoscale_error_releases_gate_and_preserves_original_error() -> DaftResult<()> {
        assert_dispatch_error_releases_gate(
            &[],
            MockWorkerManager::set_fail_autoscale,
            "injected autoscale failure",
        )
        .await
    }

    #[tokio::test]
    async fn test_submit_error_releases_gate_and_preserves_original_error() -> DaftResult<()> {
        assert_dispatch_error_releases_gate(
            &[(Arc::from("worker1"), 1)],
            MockWorkerManager::set_fail_submit,
            "injected submit failure",
        )
        .await
    }

    #[tokio::test]
    async fn test_cleanup_failure_preserves_successful_scheduler_result() -> DaftResult<()> {
        let manager = Arc::new(MockWorkerManager::new(setup_workers(&[])));
        manager.enable_dispatch_gate_checks();
        manager.set_fail_clear_demand(true);
        let (loop_state, handle) = unspawned_scheduler_loop(manager.clone());
        let demand_id = loop_state.autoscale_demand_id;
        drop(handle);

        loop_state.run().await?;
        assert_eq!(manager.clear_demand_call_count(), 1);
        assert_eq!(manager.cleared_demand_ids(), vec![demand_id]);
        assert!(manager.dispatch_gate.try_lock().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_dispatch_wins_reaper_between_snapshot_and_submit() -> DaftResult<()> {
        let worker_id: WorkerId = Arc::from("worker1");
        let manager = Arc::new(MockWorkerManager::new(setup_workers(&[(
            worker_id.clone(),
            1,
        )])));
        manager.enable_dispatch_gate_checks();
        let (mut loop_state, handle) = unspawned_scheduler_loop(manager.clone());
        let submitted = enqueue_mock_task(&mut loop_state, 0)?;
        let observation = Arc::new(Mutex::new(None));
        let reaper_observation = observation.clone();
        let reaper_manager = manager.clone();
        let reaper_worker_id = worker_id.clone();
        manager.set_after_snapshot_hook(move || {
            let barrier = Arc::new(Barrier::new(2));
            let reaper_barrier = barrier.clone();
            let reaper = std::thread::spawn(move || {
                reaper_barrier.wait();
                let gate_held = matches!(
                    reaper_manager.dispatch_gate.try_lock(),
                    Err(TryLockError::WouldBlock)
                );
                let retired = reaper_manager.try_reap_idle_worker(&reaper_worker_id);
                (gate_held, retired)
            });
            barrier.wait();
            // Joining inside the snapshot hook guarantees the attempted retirement
            // happens before submit, while the worker-map lock is already released.
            *reaper_observation.lock().unwrap() = Some(reaper.join().unwrap());
        });

        // No standalone test guard: the scheduler itself must acquire the gate.
        loop_state.schedule_and_dispatch()?;
        assert_eq!(*observation.lock().unwrap(), Some((true, false)));
        assert_eq!(loop_state.scheduler.num_pending_tasks(), 0);
        assert!(loop_state.dispatcher.has_running_tasks());
        assert!(manager.dispatch_gate.try_lock().is_ok());
        // Once dispatch releases the gate, registered active work prevents reaping.
        assert!(!manager.try_reap_idle_worker(&worker_id));

        drop(handle);
        loop_state.run().await?;
        assert!(submitted.await?.is_some());
        // The mock reaper is functional: it can retire this worker after completion.
        assert!(manager.try_reap_idle_worker(&worker_id));
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_actor_basic_task() -> DaftResult<()> {
        let test_context = setup_scheduler_actor_test_context(&[(Arc::from("worker1"), 1)]);

        let partition_ref = create_mock_partition_ref(100, 1024);
        let task = MockTaskBuilder::new(partition_ref.clone()).build();

        let submittable_task = SubmittableTask::task_only(task);
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
            let submittable_task = SubmittableTask::task_only(task);
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
                    let num_rows = rand::rng().random_range(100..1000);
                    let num_bytes = rand::rng().random_range(1024..1024 * 10);
                    let task_duration =
                        std::time::Duration::from_millis(rand::rng().random_range(50..150));
                    let task = MockTaskBuilder::new(create_mock_partition_ref(num_rows, num_bytes))
                        .with_task_id(submitter_id * num_tasks_per_submitter + task_id)
                        .with_sleep_duration(task_duration)
                        .build();
                    let submittable_task = SubmittableTask::task_only(task);
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

        let submittable_task = SubmittableTask::task_only(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
        drop(submitted_task);
        // Notifier may not fire if the scheduler filtered the task before dispatch.
        let _ = cancel_receiver.await;

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
                    let should_cancel = rand::rng().random_bool(0.5);

                    let num_rows = rand::rng().random_range(100..1000);
                    let num_bytes = rand::rng().random_range(1024..1024 * 10);
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
                        let submittable_task = SubmittableTask::task_only(task);
                        let submitted_task = submittable_task.submit(&scheduler_handle)?;
                        submitted_task_tx
                            .send((submitted_task, num_rows, num_bytes, Some(cancel_receiver)))
                            .await
                            .unwrap();
                    } else {
                        let task_duration =
                            std::time::Duration::from_millis(rand::rng().random_range(50..150));
                        let task = task.with_sleep_duration(task_duration);
                        let task = task.build();
                        let submittable_task = SubmittableTask::task_only(task);
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
                // Notifier may not fire if scheduler-side filtering kicked in.
                let _ = cancel_receiver.await;
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
        let submittable_task = SubmittableTask::task_only(task);
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
        let submittable_task = SubmittableTask::task_only(task);
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
        let submittable_task = SubmittableTask::task_only(task);
        let submitted_task = submittable_task.submit(&test_context.scheduler_handle_ref)?;
        let result = submitted_task.await?;
        assert_eq!(result.unwrap().partitions().len(), 1);

        test_context.cleanup().await?;
        Ok(())
    }
}
