//! Safe, externally-driven execution of an already selected local plan.
//!
//! PostgreSQL remains the only owner of PostgreSQL executor state.  This
//! wrapper accepts owned Daft batches through bounded queues, runs the existing
//! Swordfish pipeline on Daft worker threads, and exposes non-blocking output.

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_metrics::{QueryEndState, QueryID, snapshot::StatSnapshotImpl};
use common_runtime::{
    RuntimeRef, RuntimeTask, get_compute_runtime, get_or_init_compute_runtime_num_worker_threads,
    set_compute_runtime_num_worker_threads,
};
use daft_local_plan::{LocalPhysicalPlanRef, SourceId};
use daft_micropartition::MicroPartitionRef;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    ExecutionRuntimeContext,
    input_sender::InputSender,
    pipeline::{BuilderContext, PipelineMessage, translate_physical_plan_to_pipeline},
    resource_manager::MemoryManager,
    runtime_stats::RuntimeStatsManager,
    sources::streaming::{StreamingBuildPushError, StreamingInputSender, StreamingPushResult},
    spill::SpillManager,
};

static PREPARED_WORKER_THREADS: OnceLock<usize> = OnceLock::new();

/// Configuration copied by the bridge before starting a prepared pipeline.
#[derive(Debug, Clone)]
pub struct PreparedExecutionConfig {
    /// Number of Daft compute workers fixed by the first execution in a process.
    pub worker_threads: usize,
    /// Per-source input queue and output queue capacity.
    pub queue_depth: usize,
    /// Execution-local memory budget in bytes.
    pub memory_limit_bytes: u64,
    /// Secure per-handle directory used by spill-capable operators.
    pub spill_dir: Option<PathBuf>,
    /// Preserve input order where the selected operators support it.
    pub maintain_order: bool,
}

/// Terminal and non-terminal execution states visible to the bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    /// Inputs and outputs may still make progress.
    Running,
    /// The output producer closed normally and all output was consumed.
    Completed,
    /// Cancellation was requested.
    Cancelled,
    /// A pipeline or protocol error made the execution unusable.
    Failed,
}

/// Result of a non-blocking input push.
#[derive(Debug)]
pub enum PushBatchResult {
    /// The input queue accepted the batch and now owns it.
    Accepted,
    /// The bounded input queue is full.
    Again(MicroPartitionRef),
    /// The source or execution has ended.
    End(MicroPartitionRef),
    /// The source id is invalid.
    Error(MicroPartitionRef),
}

impl PushBatchResult {
    /// Return the unconsumed batch for every non-success result.
    pub fn into_unconsumed_batch(self) -> Option<MicroPartitionRef> {
        match self {
            Self::Accepted => None,
            Self::Again(batch) | Self::End(batch) | Self::Error(batch) => Some(batch),
        }
    }
}

/// Result of reserving input capacity and then constructing/importing a batch.
#[derive(Debug)]
pub enum BuildPushResult<E> {
    /// Capacity was reserved, construction succeeded, and the queue owns it.
    Accepted,
    /// No capacity was reserved; the construction callback was not invoked.
    Again,
    /// The source or execution has ended; the callback was not invoked.
    End,
    /// The source id is invalid; the callback was not invoked.
    InvalidSource,
    /// Construction failed after reserving capacity but before enqueueing.
    BuildError(E),
}

/// Result of a non-blocking Finish operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishResult {
    /// This call enqueued the source's one Finish message.
    Accepted,
    /// The queue is full; Finish was not accepted and may be retried.
    Again,
    /// Finish had already succeeded, or execution was cancelled/ended.
    End,
    /// The source id is invalid.
    Error,
}

/// Result of an immediate or bounded output poll.
#[derive(Debug)]
pub enum NextResult {
    /// One output batch is ready and ownership transfers to the caller.
    Ready(MicroPartitionRef),
    /// No output is currently ready; execution may still make progress.
    NotReady,
    /// The output producer closed normally; this result is permanent.
    Done,
    /// Cancellation was requested; this result is permanent.
    Cancelled,
    /// Execution entered a pipeline/protocol error state.
    Failed,
}

/// Result of waiting for an event without consuming an output batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitResult {
    /// Output or another observable state transition is available.
    Progress,
    /// The caller-supplied bound expired.
    Timeout,
    /// Execution completed normally.
    Done,
    /// Execution was cancelled.
    Cancelled,
    /// Execution failed.
    Failed,
}

/// Sanitized execution-local metrics for the PostgreSQL bridge.
///
/// `operator_compute_us` is the sum of active durations reported by all
/// Swordfish nodes. Those durations can overlap across workers and pipelines;
/// it is not an additive wall-clock segment. Memory values are reservations
/// tracked by this execution's [`MemoryManager`], not process RSS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PreparedExecutionMetrics {
    /// Sum of active operator durations across all local nodes.
    pub operator_compute_us: u64,
    /// Bytes currently reserved from the execution-local manager.
    pub current_memory_bytes: u64,
    /// Maximum simultaneously reserved bytes observed by that manager.
    pub peak_memory_bytes: u64,
    /// Cumulative bytes written to completed spill runs and merge outputs.
    pub spill_bytes: u64,
    /// Number of completed spill run and merge output files.
    pub spill_count: u64,
}

struct PreparedExecutionInner {
    state: ExecutionState,
    input_senders: HashMap<SourceId, StreamingInputSender>,
    output_receiver: Option<mpsc::Receiver<MicroPartitionRef>>,
    pending_output: VecDeque<MicroPartitionRef>,
    batches_sent: u64,
    batches_received: u64,
}

/// Non-blocking Rust wrapper used behind the opaque C bridge handle.
pub struct PreparedNativeExecution {
    inner: Mutex<PreparedExecutionInner>,
    cancellation: CancellationToken,
    pipeline_error: Arc<Mutex<Option<String>>>,
    task: Mutex<Option<RuntimeTask<()>>>,
    stats_handle: Option<crate::runtime_stats::RuntimeStatsManagerHandle>,
    memory_manager: Option<Arc<MemoryManager>>,
    spill_manager: Option<Arc<SpillManager>>,
    final_metrics: Mutex<Option<PreparedExecutionMetrics>>,
}

impl PreparedNativeExecution {
    const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(2);

    /// Build a channel-only wrapper for focused queue/lifecycle tests.
    pub fn new(
        input_sender: StreamingInputSender,
        output_receiver: mpsc::Receiver<MicroPartitionRef>,
    ) -> Self {
        Self::from_parts(
            HashMap::from([(0, input_sender)]),
            output_receiver,
            CancellationToken::new(),
            Arc::new(Mutex::new(None)),
            None,
            None,
            None,
            None,
        )
    }

    /// Construct and start the real Swordfish pipeline for a physical plan.
    ///
    /// No runtime is initialized until this function is called.  Capability
    /// query functions therefore remain thread-free.
    pub fn try_prepare(
        plan: LocalPhysicalPlanRef,
        config: PreparedExecutionConfig,
    ) -> DaftResult<Self> {
        if config.worker_threads == 0 {
            return Err(DaftError::ValueError(
                "prepared execution worker_threads must be positive".to_string(),
            ));
        }
        if config.queue_depth == 0 {
            return Err(DaftError::ValueError(
                "prepared execution queue_depth must be positive".to_string(),
            ));
        }
        if config.memory_limit_bytes == 0 {
            return Err(DaftError::ValueError(
                "prepared execution memory_limit_bytes must be positive".to_string(),
            ));
        }

        // Freeze the worker count before pipeline translation. Some existing
        // pipeline builders consult the compute parallelism and would
        // otherwise initialize that setting to the machine CPU count before
        // the bridge can apply the backend's configured value.
        let runtime = configured_compute_runtime(config.worker_threads)?;

        let exec_cfg = Arc::new(DaftExecutionConfig {
            maintain_order: config.maintain_order,
            ..Default::default()
        });
        // The calibrated P1 cost profile admits at most half of the fragment
        // budget as retained blocking state. Keep the other half available for
        // bounded source batches and the materialized output of Sort/TopN.
        let fragment_memory_limit = usize::try_from(config.memory_limit_bytes).map_err(|_| {
            DaftError::ValueError("prepared execution memory_limit_bytes exceeds usize".to_string())
        })?;
        let blocking_retained_limit = (fragment_memory_limit / 2).max(1);
        let spill_manager = config
            .spill_dir
            .clone()
            .map(SpillManager::new)
            .transpose()?
            .map(Arc::new);
        let mut builder_context = BuilderContext::new()
            .with_streaming_queue_depth(config.queue_depth)
            .with_blocking_sink_retained_limit_bytes(blocking_retained_limit);
        if let Some(manager) = &spill_manager {
            builder_context = builder_context.with_spill_manager(manager.clone());
        }
        let (pipeline, all_input_senders) =
            translate_physical_plan_to_pipeline(plan.as_ref(), &exec_cfg, &builder_context)?;

        let mut input_senders = HashMap::with_capacity(all_input_senders.len());
        for (source_id, sender) in all_input_senders {
            match sender {
                InputSender::Streaming(sender) => {
                    input_senders.insert(source_id, sender);
                }
                _ => {
                    return Err(DaftError::ValueError(
                        "prepared execution accepts only StreamingScan inputs".to_string(),
                    ));
                }
            }
        }
        if input_senders.is_empty() {
            return Err(DaftError::ValueError(
                "prepared execution requires at least one StreamingScan".to_string(),
            ));
        }

        let query_id: QueryID = "pg-daft".into();
        let stats_manager = RuntimeStatsManager::try_new(
            runtime.runtime.handle(),
            &pipeline,
            vec![],
            query_id,
            false,
        )?;
        let stats_handle = stats_manager.handle();
        let memory_manager = Arc::new(MemoryManager::with_limit(config.memory_limit_bytes));
        let execution_stats_handle = stats_handle.clone();
        let execution_memory_manager = memory_manager.clone();
        let execution_spill_manager = spill_manager.clone();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let pipeline_error = Arc::new(Mutex::new(None));
        let task_error = pipeline_error.clone();
        let (output_sender, output_receiver) = mpsc::channel(config.queue_depth);

        let task = runtime.spawn(async move {
            let mut runtime_context = ExecutionRuntimeContext::new(memory_manager, stats_handle);
            let mut output = match pipeline.start(config.maintain_order, &mut runtime_context) {
                Ok(output) => output,
                Err(error) => {
                    *task_error.lock().unwrap() = Some(error.to_string());
                    stats_manager.finish(QueryEndState::Failed).await;
                    return;
                }
            };

            let mut end_state = QueryEndState::Finished;
            loop {
                tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => {
                        end_state = QueryEndState::Canceled;
                        break;
                    }
                    worker = runtime_context.join_next() => {
                        match worker {
                            Some(Ok(())) => {}
                            Some(Err(error)) => {
                                *task_error.lock().unwrap() = Some(error.to_string());
                                end_state = QueryEndState::Failed;
                                break;
                            }
                            None => break,
                        }
                    }
                    message = output.recv() => {
                        match message {
                            Some(PipelineMessage::Morsel { partition, .. }) => {
                                if output_sender.send(Arc::new(partition)).await.is_err() {
                                    end_state = QueryEndState::Canceled;
                                    break;
                                }
                            }
                            Some(PipelineMessage::Flush(_)) => {}
                            Some(PipelineMessage::FlightPartitionRef { .. }) => {
                                *task_error.lock().unwrap() = Some(
                                    "prepared execution received unsupported Flight output".to_string(),
                                );
                                end_state = QueryEndState::Failed;
                                break;
                            }
                            None => break,
                        }
                    }
                }
            }

            if let Err(error) = runtime_context.shutdown().await
                && task_error.lock().unwrap().is_none()
            {
                *task_error.lock().unwrap() = Some(error.to_string());
                end_state = QueryEndState::Failed;
            }
            stats_manager.finish(end_state).await;
        });

        Ok(Self::from_parts(
            input_senders,
            output_receiver,
            cancellation,
            pipeline_error,
            Some(task),
            Some(execution_stats_handle),
            Some(execution_memory_manager),
            execution_spill_manager,
        ))
    }

    fn from_parts(
        input_senders: HashMap<SourceId, StreamingInputSender>,
        output_receiver: mpsc::Receiver<MicroPartitionRef>,
        cancellation: CancellationToken,
        pipeline_error: Arc<Mutex<Option<String>>>,
        task: Option<RuntimeTask<()>>,
        stats_handle: Option<crate::runtime_stats::RuntimeStatsManagerHandle>,
        memory_manager: Option<Arc<MemoryManager>>,
        spill_manager: Option<Arc<SpillManager>>,
    ) -> Self {
        Self {
            inner: Mutex::new(PreparedExecutionInner {
                state: ExecutionState::Running,
                input_senders,
                output_receiver: Some(output_receiver),
                pending_output: VecDeque::new(),
                batches_sent: 0,
                batches_received: 0,
            }),
            cancellation,
            pipeline_error,
            task: Mutex::new(task),
            stats_handle,
            memory_manager,
            spill_manager,
            final_metrics: Mutex::new(None),
        }
    }

    /// Try to transfer one already-owned batch without waiting.
    pub fn try_push_batch(&self, source_id: u32, batch: MicroPartitionRef) -> PushBatchResult {
        let retry = batch.clone();
        match self.try_push_batch_with(source_id, || Ok::<_, std::convert::Infallible>(batch)) {
            BuildPushResult::Accepted => PushBatchResult::Accepted,
            BuildPushResult::Again => PushBatchResult::Again(retry),
            BuildPushResult::End => PushBatchResult::End(retry),
            BuildPushResult::InvalidSource => PushBatchResult::Error(retry),
            BuildPushResult::BuildError(never) => match never {},
        }
    }

    /// Reserve source capacity before invoking a foreign-memory importer.
    pub fn try_push_batch_with<E, F>(&self, source_id: u32, build: F) -> BuildPushResult<E>
    where
        F: FnOnce() -> Result<MicroPartitionRef, E>,
    {
        let mut inner = self.inner.lock().unwrap();
        if inner.state != ExecutionState::Running {
            return BuildPushResult::End;
        }
        let Some(sender) = inner.input_senders.get(&source_id) else {
            inner.state = ExecutionState::Failed;
            return BuildPushResult::InvalidSource;
        };

        match sender.try_push_batch_with(0, 0, build) {
            Ok(()) => {
                inner.batches_sent += 1;
                BuildPushResult::Accepted
            }
            Err(StreamingBuildPushError::Again) => BuildPushResult::Again,
            Err(StreamingBuildPushError::End) => BuildPushResult::End,
            Err(StreamingBuildPushError::InvalidSource) => {
                inner.state = ExecutionState::Failed;
                BuildPushResult::InvalidSource
            }
            Err(StreamingBuildPushError::Build(error)) => BuildPushResult::BuildError(error),
        }
    }

    /// Try to enqueue one source Finish without waiting.
    pub fn try_finish_source(&self, source_id: u32) -> FinishResult {
        let mut inner = self.inner.lock().unwrap();
        if inner.state != ExecutionState::Running {
            return FinishResult::End;
        }
        let Some(sender) = inner.input_senders.get(&source_id) else {
            inner.state = ExecutionState::Failed;
            return FinishResult::Error;
        };

        match sender.try_finish_source(0) {
            StreamingPushResult::Ok => FinishResult::Accepted,
            StreamingPushResult::Again => FinishResult::Again,
            StreamingPushResult::End => FinishResult::End,
            StreamingPushResult::Error => {
                inner.state = ExecutionState::Failed;
                FinishResult::Error
            }
        }
    }

    /// Poll one output batch without waiting.
    pub fn try_next(&self) -> NextResult {
        let mut inner = self.inner.lock().unwrap();
        match inner.state {
            ExecutionState::Cancelled => return NextResult::Cancelled,
            ExecutionState::Failed => return NextResult::Failed,
            ExecutionState::Completed => return NextResult::Done,
            ExecutionState::Running => {}
        }
        if let Some(batch) = inner.pending_output.pop_front() {
            inner.batches_received += 1;
            return NextResult::Ready(batch);
        }

        let Some(receiver) = inner.output_receiver.as_mut() else {
            return self.mark_disconnected(&mut inner);
        };
        match receiver.try_recv() {
            Ok(batch) => {
                inner.batches_received += 1;
                NextResult::Ready(batch)
            }
            Err(mpsc::error::TryRecvError::Empty) => NextResult::NotReady,
            Err(mpsc::error::TryRecvError::Disconnected) => self.mark_disconnected(&mut inner),
        }
    }

    fn mark_disconnected(&self, inner: &mut PreparedExecutionInner) -> NextResult {
        inner.output_receiver = None;
        if self.pipeline_error.lock().unwrap().is_some() {
            inner.state = ExecutionState::Failed;
            NextResult::Failed
        } else {
            inner.state = ExecutionState::Completed;
            NextResult::Done
        }
    }

    fn inspect_event(&self) -> WaitResult {
        let mut inner = self.inner.lock().unwrap();
        match inner.state {
            ExecutionState::Completed => return WaitResult::Done,
            ExecutionState::Cancelled => return WaitResult::Cancelled,
            ExecutionState::Failed => return WaitResult::Failed,
            ExecutionState::Running => {}
        }
        if !inner.pending_output.is_empty() {
            return WaitResult::Progress;
        }
        let Some(receiver) = inner.output_receiver.as_mut() else {
            return match self.mark_disconnected(&mut inner) {
                NextResult::Done => WaitResult::Done,
                NextResult::Failed => WaitResult::Failed,
                _ => unreachable!(),
            };
        };
        match receiver.try_recv() {
            Ok(batch) => {
                inner.pending_output.push_back(batch);
                WaitResult::Progress
            }
            Err(mpsc::error::TryRecvError::Empty) => WaitResult::Timeout,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                match self.mark_disconnected(&mut inner) {
                    NextResult::Done => WaitResult::Done,
                    NextResult::Failed => WaitResult::Failed,
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Wait for an event without consuming an output batch.
    pub async fn wait_for_event(&self, timeout: Duration) -> WaitResult {
        let started = Instant::now();
        loop {
            let event = self.inspect_event();
            if event != WaitResult::Timeout {
                return event;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return WaitResult::Timeout;
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return WaitResult::Cancelled,
                _ = tokio::time::sleep(remaining.min(Self::WAIT_POLL_INTERVAL)) => {}
            }
        }
    }

    /// Synchronous bridge helper for a bounded event wait.
    ///
    /// PostgreSQL calls this from its ordinary backend thread, outside Tokio.
    pub fn wait_for_event_blocking(&self, timeout: Duration) -> WaitResult {
        get_compute_runtime().block_on_current_thread(self.wait_for_event(timeout))
    }

    /// Wait for and consume one output batch, cancellation, or timeout.
    pub async fn wait_next(&self, timeout: Duration) -> NextResult {
        let started = Instant::now();
        loop {
            let result = self.try_next();
            if !matches!(result, NextResult::NotReady) {
                return result;
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return NextResult::NotReady;
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return NextResult::Cancelled,
                _ = tokio::time::sleep(remaining.min(Self::WAIT_POLL_INTERVAL)) => {}
            }
        }
    }

    /// Idempotently cancel input, output, and bounded waiters.
    pub fn cancel(&self) {
        self.cancellation.cancel();
        let mut inner = self.inner.lock().unwrap();
        if inner.state == ExecutionState::Running {
            inner.state = ExecutionState::Cancelled;
            for sender in inner.input_senders.values() {
                sender.cancel();
            }
            inner.output_receiver = None;
            inner.pending_output.clear();
        }
    }

    /// Cancel and synchronously wait until the owning pipeline task has
    /// released all execution resources, including open spill files.
    pub fn cancel_and_wait(&self) {
        self.cancel();
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            let _ = get_compute_runtime().block_on_current_thread(task);
        }
    }

    /// Current lifecycle state.
    pub fn state(&self) -> ExecutionState {
        self.inner.lock().unwrap().state
    }

    /// Accepted input and returned output batch counts.
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.batches_sent, inner.batches_received)
    }

    /// Return a read-only metrics snapshot without changing execution state.
    ///
    /// Final metrics are cached because the runtime stats manager releases its
    /// completed per-input snapshot after the first read. Running snapshots
    /// remain live and are intentionally not cached.
    pub fn metrics(&self) -> DaftResult<PreparedExecutionMetrics> {
        if let Some(metrics) = *self.final_metrics.lock().unwrap() {
            return Ok(metrics);
        }

        let (current_memory_bytes, peak_memory_bytes) = self
            .memory_manager
            .as_ref()
            .map(|manager| manager.reservation_snapshot())
            .unwrap_or((0, 0));
        let operator_compute_us = if let Some(stats_handle) = &self.stats_handle {
            let stats = get_compute_runtime()
                .block_on_current_thread(stats_handle.take_input_snapshot(0))?;
            stats
                .nodes
                .iter()
                .map(|(_, snapshot)| snapshot.duration_us())
                .sum()
        } else {
            0
        };
        let spill = self
            .spill_manager
            .as_ref()
            .map(|manager| manager.metrics())
            .unwrap_or_default();
        let metrics = PreparedExecutionMetrics {
            operator_compute_us,
            current_memory_bytes,
            peak_memory_bytes,
            spill_bytes: spill.bytes,
            spill_count: spill.count,
        };

        if self.state() != ExecutionState::Running {
            *self.final_metrics.lock().unwrap() = Some(metrics);
        }
        Ok(metrics)
    }

    /// Sanitized pipeline failure, if execution ended abnormally.
    pub fn last_error(&self) -> Option<String> {
        self.pipeline_error.lock().unwrap().clone()
    }
}

fn configured_compute_runtime(worker_threads: usize) -> DaftResult<RuntimeRef> {
    if let Some(configured) = PREPARED_WORKER_THREADS.get() {
        if *configured != worker_threads {
            return Err(DaftError::ValueError(format!(
                "Daft worker thread count is already fixed at {configured}, requested {worker_threads}"
            )));
        }
        return Ok(get_compute_runtime());
    }

    match set_compute_runtime_num_worker_threads(worker_threads) {
        Ok(()) => {}
        Err(_) if get_or_init_compute_runtime_num_worker_threads() == worker_threads => {}
        Err(_) => {
            return Err(DaftError::ValueError(format!(
                "Daft compute runtime was initialized with {} workers, requested {worker_threads}",
                get_or_init_compute_runtime_num_worker_threads()
            )));
        }
    }
    let _ = PREPARED_WORKER_THREADS.set(worker_threads);
    Ok(get_compute_runtime())
}

impl Drop for PreparedNativeExecution {
    fn drop(&mut self) {
        self.cancel_and_wait();
    }
}
