use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::DaftResult;
use common_metrics::{QueryEndState, QueryID};
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{
    DaftContext, Subscriber,
    subscribers::{
        Event, event_header,
        events::{TaskInfo, TaskStartEvent},
    },
};
use daft_local_plan::{ExecutionStats, Input, InputId, LocalPhysicalPlanRef, SourceId, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::server::flight_server::{
    FlightServerConnectionHandle, ShuffleFlightServer, start_server_loop,
};
use futures::{FutureExt, future::BoxFuture};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_local_plan::python::PyExecutionStats,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    daft_partition_refs::PyFlightPartitionRef,
    pyo3::{
        Bound, IntoPyObject, PyAny, PyRef, PyResult, Python, pyclass, pymethods, sync::MutexExt,
    },
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Sender, UnboundedSender, create_channel, create_unbounded_channel},
    pipeline::{
        BuilderContext, MapperAttempt, MapperAttemptRegistry, PipelineMessage,
        translate_physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid,
    },
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{RuntimeStatsManager, RuntimeStatsManagerHandle},
};

enum ExecutionEngineResultItem {
    Partition(MicroPartition),
    FlightPartitionRef(FlightPartitionRef),
}

/// Global tokio runtime shared by all NativeExecutor instances
static GLOBAL_RUNTIME: OnceLock<Handle> = OnceLock::new();

/// Get or initialize the global tokio runtime
#[cfg(feature = "python")]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_all();
        pyo3_async_runtimes::tokio::init(builder);
        std::thread::spawn(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(futures::future::pending::<()>());
        });
        pyo3_async_runtimes::tokio::get_runtime().handle().clone()
    })
}

#[cfg(not(feature = "python"))]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build global tokio runtime for NativeExecutor");
        let handle = rt.handle().clone();
        // Keep the runtime alive for the duration of the process.
        std::thread::spawn(move || {
            rt.block_on(futures::future::pending::<()>());
        });
        handle
    })
}

/// Message sent to the execution task to enqueue inputs
pub(crate) struct EnqueueInputMessage {
    /// The input_id for this enqueue operation
    input_id: InputId,
    /// Plan inputs grouped by source_id
    inputs: HashMap<SourceId, Input>,
    /// Sender for results of this input_id
    result_sender: UnboundedSender<ExecutionEngineResultItem>,
}

/// Routes pipeline messages to per-input_id channels.
struct MessageRouter {
    output_senders: HashMap<InputId, UnboundedSender<ExecutionEngineResultItem>>,
    /// Wall-clock start instant when each `input_id` was enqueued to the pipeline.
    input_start_times: HashMap<InputId, Instant>,
}

impl MessageRouter {
    fn new() -> Self {
        Self {
            output_senders: HashMap::new(),
            input_start_times: HashMap::new(),
        }
    }

    /// Route a message to the appropriate channel based on its input_id.
    fn route_message(&mut self, msg: PipelineMessage) {
        match msg {
            PipelineMessage::Flush(input_id) => {
                self.input_start_times.remove(&input_id);
                self.output_senders.remove(&input_id);
            }
            PipelineMessage::Morsel {
                input_id,
                partition,
            } => {
                if let Some(sender) = self.output_senders.get(&input_id) {
                    let _ = sender.send(ExecutionEngineResultItem::Partition(partition));
                }
            }
            PipelineMessage::FlightPartitionRef {
                input_id,
                partition_ref,
            } => {
                if let Some(sender) = self.output_senders.get(&input_id) {
                    let _ =
                        sender.send(ExecutionEngineResultItem::FlightPartitionRef(partition_ref));
                }
            }
        }
    }

    fn insert_output_sender(
        &mut self,
        input_id: InputId,
        sender: UnboundedSender<ExecutionEngineResultItem>,
    ) {
        self.input_start_times.insert(input_id, Instant::now());
        self.output_senders.insert(input_id, sender);
    }
}

impl Drop for MessageRouter {
    fn drop(&mut self) {
        for (input_id, started) in self.input_start_times.drain() {
            log::debug!(
                "NativeExecutor: input_id={input_id} ended without Flush after {:?} (cancel/shutdown?)",
                started.elapsed()
            );
        }
    }
}

/// Per-plan execution state
struct PlanState {
    task_handle: RuntimeTask<DaftResult<()>>,
    enqueue_input_sender: Sender<EnqueueInputMessage>,
    stats_handle: RuntimeStatsManagerHandle,
    active_input_ids: HashSet<InputId>,
    skipped_corrupt_files: Arc<std::sync::Mutex<Vec<(String, String, bool)>>>,
    /// Set by [`run_execution_loop`] right before it exits, on **every** exit
    /// path. Lets a late `enqueue` (which only sees a closed channel) decide by
    /// type whether the race is retriable ([`PlanEnd::Finished`]) or terminal,
    /// and report *why* — rather than parsing an error string. This is the only
    /// reliable channel in distributed (flotilla) mode: the worker's
    /// stdout/stderr are not collected, so the reason must travel back through
    /// the task result to reach the driver.
    end_reason: Arc<Mutex<Option<PlanEnd>>>,
    /// Per-pipeline registry of shuffle map task attempts, keyed by `InputId`.
    /// Shared with the pipeline's Celeborn sink; entries are removed on
    /// `try_finish` so it does not grow unbounded on a reused pipeline.
    map_attempts: MapperAttemptRegistry,
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "NativeExecutor", frozen)
)]
pub struct PyNativeExecutor {
    executor: Arc<Mutex<NativeExecutor>>,
    address: Option<String>,
}

#[cfg(feature = "python")]
impl Default for PyNativeExecutor {
    fn default() -> Self {
        Self::new(false, "")
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PyNativeExecutor {
    #[new]
    pub fn new(is_flotilla_worker: bool, ip: &str) -> Self {
        let executor = NativeExecutor::new(is_flotilla_worker, ip);
        let address = executor.shuffle_address();
        Self {
            executor: Arc::new(Mutex::new(executor)),
            address,
        }
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.address.clone()
    }

    /// Connect to a Celeborn LifecycleManager and set the resulting client
    /// on this executor.
    ///
    /// Configuration is read from the `DaftExecutionConfig.celeborn` field.
    /// Must be called before any shuffle task that uses the Celeborn backend.
    /// Idempotent: if a client is already connected, this is a no-op. The
    /// early-out below is only a fast path — two callers racing past it both
    /// connect, and `NativeExecutor::set_celeborn_client` keeps whichever
    /// arrives first rather than swapping out a client already in use, so the
    /// loser's connection is simply dropped.
    ///
    /// # Arguments
    /// * `daft_execution_config` - A `PyDaftExecutionConfig` whose inner
    ///   `celeborn` field supplies the connection parameters (lm_host,
    ///   lm_port, app_id, and native `celeborn.*` properties).
    #[cfg(feature = "celeborn")]
    pub fn set_celeborn_client(
        &self,
        py: Python<'_>,
        daft_execution_config: &PyDaftExecutionConfig,
    ) -> PyResult<()> {
        if self
            .executor
            .lock_py_attached(py)
            .unwrap()
            .has_celeborn_client()
        {
            return Ok(());
        }

        let exec_config = daft_execution_config.config.as_ref();
        let celeborn_cfg = exec_config.celeborn.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "DaftExecutionConfig.celeborn is None; \
                 set celeborn config via with_config_values() before calling set_celeborn_client()",
            )
        })?;
        if !celeborn_cfg.is_complete() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "DaftExecutionConfig.celeborn is missing the LifecycleManager coordinates \
                 (lm_host={:?}, lm_port={}); set them with \
                 daft.context.set_execution_config(celeborn_lm_host=..., celeborn_lm_port=...)",
                celeborn_cfg.lm_host, celeborn_cfg.lm_port
            )));
        }

        let client_config = daft_shuffles::client::celeborn::CelebornClientConfig {
            lm_host: celeborn_cfg.lm_host.clone(),
            lm_port: celeborn_cfg.lm_port,
            app_id: celeborn_cfg.app_id.clone(),
            properties: celeborn_cfg.properties.clone(),
        };
        // `connect` is a blocking RPC to the LifecycleManager, so detach from
        // the interpreter for it: this runs on the worker's main Python thread
        // and would otherwise stall every other thread in the process for the
        // round-trip.
        let client = py
            .detach(|| daft_shuffles::client::celeborn::connect_celeborn_client(&client_config))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to connect to Celeborn LifecycleManager at {}:{}: {e}",
                    celeborn_cfg.lm_host, celeborn_cfg.lm_port
                ))
            })?;
        self.executor
            .lock_py_attached(py)
            .unwrap()
            .set_celeborn_client(client);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (local_physical_plan, daft_ctx, input_id, inputs, context=None, maintain_order=true))]
    pub fn run<'py>(
        &self,
        py: Python<'py>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        daft_ctx: &PyDaftContext,
        input_id: InputId,
        inputs: HashMap<SourceId, Input>,
        context: Option<HashMap<String, String>>,
        maintain_order: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let daft_ctx: &DaftContext = daft_ctx.into();
        let plan = local_physical_plan.plan.clone();
        let exec_cfg = daft_ctx.execution_config();
        let subscribers = daft_ctx.subscribers();
        let executor = self.executor.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // A task reuses a pipeline keyed by `fingerprint`. That pipeline can
            // finish (all currently-active inputs drained) and start tearing down
            // in the tiny window between `run()` observing the plan as present and
            // the detached `enqueue` actually sending — the input then lands on a
            // just-closed channel and fails with "cannot enqueue new input".
            //
            // This is benign and recoverable: drop the finished plan and rebuild
            // it, then re-enqueue. Bounded so a genuinely-broken plan still errors
            // out instead of spinning forever. Retrying here treats the symptom;
            // closing the window itself would mean holding the pipeline open until
            // the reduce stage ends, which reaches well beyond this backend.
            const MAX_ENQUEUE_ATTEMPTS: usize = 5;
            let mut attempt = 0usize;
            loop {
                attempt += 1;
                let (fingerprint, enqueue_future) = Python::attach(|py| {
                    executor.lock_py_attached(py).unwrap().run(
                        &plan,
                        exec_cfg.clone(),
                        subscribers.clone(),
                        context.clone(),
                        inputs.clone(),
                        input_id,
                        maintain_order,
                    )
                })?;

                // `?` propagates terminal failures (cancel / real error / abort)
                // unchanged — only the typed `PipelineFinished` signal retries.
                match enqueue_future.await? {
                    EnqueueOutcome::Ready(result) => {
                        return Ok(PyResultReceiver {
                            result: Arc::new(tokio::sync::Mutex::new(Some(result))),
                            fingerprint,
                            input_id,
                            executor,
                        });
                    }
                    EnqueueOutcome::PipelineFinished => {
                        if attempt >= MAX_ENQUEUE_ATTEMPTS {
                            return Err(common_error::DaftError::InternalError(format!(
                                "cannot enqueue input_id={input_id}: reused pipeline kept \
                                 finishing across {MAX_ENQUEUE_ATTEMPTS} rebuild attempts"
                            ))
                            .into());
                        }
                        // Evict the finished plan so the next attempt rebuilds it.
                        Python::attach(|py| {
                            executor
                                .lock_py_attached(py)
                                .unwrap()
                                .discard_finished_plan(fingerprint);
                        });
                    }
                }
            }
        })
    }

    pub fn active_plan_count(&self, py: Python<'_>) -> usize {
        self.executor.lock_py_attached(py).unwrap().plans.len()
    }

    pub fn cancel_plan(&self, py: Python<'_>, fingerprint: u64) -> PyResult<()> {
        self.executor
            .lock_py_attached(py)
            .unwrap()
            .cancel_plan(fingerprint);
        Ok(())
    }

    #[staticmethod]
    pub fn repr_ascii(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        simple: bool,
    ) -> PyResult<String> {
        Ok(NativeExecutor::repr_ascii(
            &logical_plan_builder.builder,
            cfg.config,
            simple,
        ))
    }

    #[staticmethod]
    pub fn repr_mermaid(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        options: MermaidDisplayOptions,
    ) -> PyResult<String> {
        Ok(NativeExecutor::repr_mermaid(
            &logical_plan_builder.builder,
            cfg.config,
            options,
        ))
    }
}

/// Returns a fingerprint that is unique for each call when the caller does not
/// supply one. Using a fixed value (e.g. 0) caused `NativeExecutor::run` to
/// reuse the cached pipeline from a prior execution when the new plan requires
/// a different `InputSender` variant, which reached the `unreachable!` branch
/// in `InputSender::send` (see GitHub issue #7087).
fn next_auto_fingerprint() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn parse_context(ctx: Option<&HashMap<String, String>>) -> (QueryID, u64, Option<u32>) {
    let query_id = ctx
        .as_ref()
        .and_then(|c| c.get("query_id"))
        .map(|s| QueryID::from(s.as_str()))
        .unwrap_or_else(|| QueryID::from(""));
    let fingerprint = ctx
        .as_ref()
        .and_then(|c| c.get("plan_fingerprint"))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(next_auto_fingerprint);
    let task_id = ctx
        .as_ref()
        .and_then(|c| c.get("task_id"))
        .and_then(|s| s.parse::<u32>().ok());

    (query_id, fingerprint, task_id)
}

/// Parse the shuffle map task attempt from the task context.
///
/// `map_id` is the upstream partition ordinal (set by the coordinator when it
/// builds the shuffle-write task); `attempt_id` is the reschedule count (set by
/// the dispatcher). Returns `None` when the task is not a shuffle mapper
/// (`map_id` absent). `attempt_id` defaults to 0 when not yet supplied.
fn parse_map_attempt(ctx: Option<&HashMap<String, String>>) -> Option<MapperAttempt> {
    let map_id = ctx?.get("map_id")?.parse::<u32>().ok()?;
    let attempt_id = ctx
        .and_then(|c| c.get("attempt_id"))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    Some(MapperAttempt { map_id, attempt_id })
}

// TODO: fix configuration for events
// This is copied from task_lifecycle.rs to avoid the daft-distributed dependency
pub fn task_events_enabled() -> bool {
    if let Ok(val) = std::env::var("DAFT_TASK_EVENTS_ENABLED") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        false // Disabled by default; enable with DAFT_TASK_EVENTS_ENABLED=true
    }
}

/// The core execution loop that drives a pipeline to completion.
/// Receives inputs via `enqueue_input_rx`, routes pipeline outputs to
/// per-input_id channels, and runs until the pipeline finishes, errors,
/// or is cancelled.
/// How a plan's execution loop ended. Recorded by [`run_execution_loop`] before
/// it returns (on every exit path), so a late `enqueue` that observes the closed
/// channel can react by *type* — not by parsing an error string.
#[derive(Clone)]
enum PlanEnd {
    /// Pipeline drained normally. A late enqueue racing this teardown is a
    /// benign lifecycle race and is safe to retry on a rebuilt plan.
    Finished,
    /// Terminal (cancelled / failed / aborted / …); not retriable. The string is
    /// a human-readable reason folded into the surfaced error.
    Terminal(String),
}

impl PlanEnd {
    fn detail(&self) -> String {
        match self {
            Self::Finished => "state=Finished".to_string(),
            Self::Terminal(s) => s.clone(),
        }
    }
}

/// Outcome of enqueueing one input into a (possibly reused) pipeline.
pub enum EnqueueOutcome {
    /// Input accepted; results will stream from the receiver.
    Ready(ExecutionEngineResult),
    /// The reused pipeline had already finished before this input landed — the
    /// caller should rebuild the plan and retry. (Distinct from a real error,
    /// which is returned as `Err`.)
    PipelineFinished,
}

async fn run_execution_loop(
    cancel: CancellationToken,
    stats_manager: RuntimeStatsManager,
    mut enqueue_input_rx: crate::channel::Receiver<EnqueueInputMessage>,
    input_senders: Arc<HashMap<SourceId, crate::input_sender::InputSender>>,
    pipeline: Box<dyn crate::pipeline::PipelineNode>,
    maintain_order: bool,
    end_reason: Arc<Mutex<Option<PlanEnd>>>,
) -> DaftResult<()> {
    let stats_manager_handle = stats_manager.handle();
    let memory_manager = get_or_init_memory_manager();
    let mut runtime_handle =
        ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
    let mut output_receiver = pipeline.start(maintain_order, &mut runtime_handle)?;

    let mut message_router = MessageRouter::new();
    let mut input_senders = Some(input_senders);
    let mut input_exhausted = false;

    let (result, finish_status) = loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                println!("Execution engine cancelled");
                break (Ok(()), QueryEndState::Canceled);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Received Ctrl-C, shutting down execution engine");
                break (Ok(()), QueryEndState::Canceled);
            }
            Some(join_result) = runtime_handle.join_next() => {
                if let Err(e) = join_result {
                    if matches!(&e, common_error::DaftError::JoinError(source) if source.is_cancelled()) {
                        break (Ok(()), QueryEndState::Canceled);
                    }
                    break (Err(e), QueryEndState::Failed);
                }
            }
            enqueue_msg = enqueue_input_rx.recv(), if !input_exhausted => {
                if let Some(EnqueueInputMessage { input_id, inputs, result_sender }) = enqueue_msg {
                    message_router.insert_output_sender(input_id, result_sender);
                    let senders = input_senders.as_ref().unwrap();
                    for (key, plan_input) in inputs {
                        if let Some(sender) = senders.get(&key) {
                            let _ = sender.send(input_id, plan_input);
                        }
                    }
                } else {
                    // All senders dropped — drop input channels so
                    // pipeline sources see EOF.
                    input_senders.take();
                    input_exhausted = true;
                }
            }
            msg = output_receiver.recv() => {
                match msg {
                    Some(msg) => {
                        message_router.route_message(msg);
                    }
                    None => {
                        // Pipeline finished. Close result channels so waiters
                        // unblock, then drain runtime tasks.
                        drop(message_router);
                        let res = runtime_handle.shutdown().await;
                        let status = if res.is_ok() {
                            QueryEndState::Finished
                        } else {
                            QueryEndState::Failed
                        };
                        break (res, status);
                    }
                }
            }
        }
    };

    stats_manager.finish(finish_status.clone()).await;
    flush_opentelemetry_providers();
    // Record how the loop ended before we return (and `enqueue_input_rx` is
    // dropped), so any subsequent `enqueue` that observes the closed channel can
    // report the reason. We record on every path — not just errors — because a
    // cancel/finish that races with a late input is otherwise invisible and
    // surfaces only as the bare "pipeline died" message.
    let plan_end = match &result {
        // Normal drain: a late enqueue racing this teardown is the benign,
        // retriable case.
        Ok(()) if matches!(finish_status, QueryEndState::Finished) => PlanEnd::Finished,
        Ok(()) => PlanEnd::Terminal(format!("state={finish_status:?}")),
        Err(e) => PlanEnd::Terminal(format!("state={finish_status:?}; root cause: {e:?}")),
    };
    *end_reason.lock().unwrap() = Some(plan_end);
    result
}

pub struct NativeExecutor {
    cancel: CancellationToken,
    is_flotilla_worker: bool,
    shuffle_server: Option<Arc<ShuffleFlightServer>>,
    shuffle_server_connection: Option<FlightServerConnectionHandle>,
    /// Global Celeborn shuffle client, shared across all queries/shuffles on
    /// this worker. Created once via [`Self::set_celeborn_client`] and injected
    /// into every [`BuilderContext`] so that pipeline nodes can use it.
    #[cfg(feature = "celeborn")]
    celeborn_client: Option<Arc<dyn daft_shuffles::client::celeborn::CelebornClient>>,
    plans: HashMap<u64, PlanState>,
}

impl NativeExecutor {
    pub fn new(is_flotilla_worker: bool, ip: &str) -> Self {
        // Determine if we are running in a flotilla worker.
        if is_flotilla_worker {
            let shuffle_server = Arc::new(ShuffleFlightServer::new());
            let shuffle_server_connection = Some(start_server_loop(ip, shuffle_server.clone()));

            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: true,
                shuffle_server: Some(shuffle_server),
                shuffle_server_connection,
                #[cfg(feature = "celeborn")]
                celeborn_client: None,
                plans: HashMap::new(),
            }
        } else {
            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: false,
                shuffle_server: None,
                shuffle_server_connection: None,
                #[cfg(feature = "celeborn")]
                celeborn_client: None,
                plans: HashMap::new(),
            }
        }
    }

    /// Set the global Celeborn shuffle client for this executor.
    ///
    /// Must be called before any shuffle task that uses the Celeborn backend.
    /// The client is injected into every `BuilderContext` created by `run()`.
    /// Idempotent: once a client is set it is never replaced.
    #[cfg(feature = "celeborn")]
    pub fn set_celeborn_client(
        &mut self,
        client: Arc<dyn daft_shuffles::client::celeborn::CelebornClient>,
    ) {
        if self.celeborn_client.is_none() {
            self.celeborn_client = Some(client);
        }
    }

    #[cfg(feature = "celeborn")]
    pub fn has_celeborn_client(&self) -> bool {
        self.celeborn_client.is_some()
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.shuffle_server_connection
            .as_ref()
            .map(|conn| conn.shuffle_address())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &mut self,
        local_physical_plan: &LocalPhysicalPlanRef,
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
        maintain_order: bool,
    ) -> DaftResult<(u64, BoxFuture<'static, DaftResult<EnqueueOutcome>>)> {
        let (query_id, fingerprint, task_id) = parse_context(additional_context.as_ref());
        let map_attempt = parse_map_attempt(additional_context.as_ref());

        if self.is_flotilla_worker {
            debug_assert_eq!(
                task_id,
                Some(input_id),
                "Flotilla invariant violated: task_id must match input_id"
            );
        }

        let task_start_dispatch = if self.is_flotilla_worker
            && task_events_enabled()
            && let Some(task_id) = task_id
        {
            Some((
                Event::TaskStart(TaskStartEvent {
                    header: event_header(query_id.clone()),
                    task: Arc::new(TaskInfo {
                        id: task_id,
                        last_node_id: 0,  // TODO: propagate last_node_id
                        node_ids: vec![], // TODO: propagate node_ids
                        plan_fingerprint: fingerprint as u32,
                        name: None,
                    }),
                    worker_id: None, // TODO: propagate worker id
                }),
                subscribers.clone(),
            ))
        } else {
            None
        };

        // Per-pipeline registry shared between this pipeline's Celeborn sink
        // (reads by `input_id` in `make_state`) and the enqueue path below
        // (writes each task's map attempt). Created on first use of the
        // fingerprint and reused for every same-fingerprint task on the shared
        // pipeline.
        let map_attempts: MapperAttemptRegistry =
            if let Some(plan_state) = self.plans.get(&fingerprint) {
                plan_state.map_attempts.clone()
            } else {
                Arc::new(std::sync::Mutex::new(HashMap::new()))
            };

        if !self.plans.contains_key(&fingerprint) {
            let cancel = self.cancel.clone();
            let additional_context = additional_context.unwrap_or_default();
            let shuffle_address = self.shuffle_address();
            let ctx = BuilderContext::new_with_context(
                query_id.clone(),
                additional_context,
                self.shuffle_server
                    .as_ref()
                    .map(|server| (server.clone(), shuffle_address.unwrap())),
            );
            #[cfg(feature = "celeborn")]
            if let Some(celeborn_client) = &self.celeborn_client {
                ctx.set_celeborn_client(celeborn_client.clone());
            }
            #[cfg(feature = "celeborn")]
            ctx.set_map_attempts(map_attempts.clone());
            let (pipeline, input_senders) =
                translate_physical_plan_to_pipeline(local_physical_plan, &exec_cfg, &ctx)?;

            let handle = get_global_runtime();
            let stats_manager = RuntimeStatsManager::try_new(
                handle,
                &pipeline,
                subscribers,
                query_id,
                self.is_flotilla_worker,
            )?;
            let stats_handle = stats_manager.handle();

            let (enqueue_input_tx, enqueue_input_rx) = create_channel::<EnqueueInputMessage>(1);

            let input_senders = Arc::new(input_senders);
            let end_reason = Arc::new(Mutex::new(None));
            let task = run_execution_loop(
                cancel,
                stats_manager,
                enqueue_input_rx,
                input_senders,
                pipeline,
                maintain_order,
                end_reason.clone(),
            );

            let task_handle = RuntimeTask::new(handle, task);
            self.plans.insert(
                fingerprint,
                PlanState {
                    task_handle,
                    enqueue_input_sender: enqueue_input_tx,
                    stats_handle,
                    active_input_ids: HashSet::new(),
                    skipped_corrupt_files: ctx.skipped_corrupt_files.clone(),
                    end_reason,
                    map_attempts,
                },
            );
        }

        let plan_state = self.plans.get_mut(&fingerprint).unwrap();
        let enqueue_input_sender = plan_state.enqueue_input_sender.clone();
        let end_reason = plan_state.end_reason.clone();
        plan_state.active_input_ids.insert(input_id);

        // Register this task's shuffle map attempt before enqueue so the sink's
        // `make_state` (which runs later, when the first morsel arrives) can look
        // it up by `input_id`. Removed in `try_finish` once the task completes.
        if let Some(map_attempt) = map_attempt
            && let Ok(mut map) = plan_state.map_attempts.lock()
        {
            map.insert(input_id, map_attempt);
        }

        Ok((
            fingerprint,
            async move {
                let (result_tx, result_rx) = create_unbounded_channel();
                let enqueue_msg = EnqueueInputMessage {
                    input_id,
                    inputs,
                    result_sender: result_tx,
                };
                if enqueue_input_sender.send(enqueue_msg).await.is_err() {
                    // The execution loop has already exited. `end_reason` was set
                    // (on every exit path) before the channel closed. Decide by
                    // type: a normal `Finished` is a retriable lifecycle race;
                    // anything else is terminal and surfaces as an error.
                    let end = end_reason.lock().unwrap().clone();
                    match end {
                        Some(PlanEnd::Finished) => {
                            return Ok(EnqueueOutcome::PipelineFinished);
                        }
                        other => {
                            let detail = other
                                .map(|e| e.detail())
                                .unwrap_or_else(|| "reason NOT recorded".to_string());
                            return Err(common_error::DaftError::InternalError(format!(
                                "Plan execution task has died; cannot enqueue new input \
                                 [fingerprint={fingerprint} input_id={input_id}] ({detail})"
                            )));
                        }
                    }
                }

                // Send the event after the task has been enqueued for execution
                if let Some((event, subscribers)) = task_start_dispatch {
                    dispatch_task_start_event(&subscribers, &event);
                }

                Ok(EnqueueOutcome::Ready(ExecutionEngineResult {
                    receiver: result_rx,
                }))
            }
            .boxed(),
        ))
    }

    /// Finish tracking an input_id. If no active input_ids remain (or the
    /// enqueue channel is closed), removes the plan and awaits the exec task.
    pub fn try_finish(
        &mut self,
        fingerprint: u64,
        input_id: InputId,
    ) -> DaftResult<BoxFuture<'static, DaftResult<ExecutionStats>>> {
        let Some(plan_state) = self.plans.get_mut(&fingerprint) else {
            // Plan already removed (pipeline died and another input_id cleaned it up).
            // Return empty stats; the actual error was already surfaced by the first caller.
            let query_id = QueryID::from("");
            return Ok(async move { Ok(ExecutionStats::new(query_id, vec![])) }.boxed());
        };

        plan_state.active_input_ids.remove(&input_id);
        // Drop this task's shuffle map attempt so the registry does not grow
        // unbounded on a reused (flotilla) pipeline.
        if let Ok(mut map) = plan_state.map_attempts.lock() {
            map.remove(&input_id);
        }
        let pipeline_dead = plan_state.enqueue_input_sender.is_closed();
        let should_remove = plan_state.active_input_ids.is_empty() || pipeline_dead;

        if should_remove {
            let plan_state = self.plans.remove(&fingerprint).unwrap();
            Ok(async move {
                // Try to get stats for this input_id. If the pipeline already died,
                // the stats manager may be finished so this can fail — that's OK.
                let stats = plan_state.stats_handle.take_input_snapshot(input_id).await;
                drop(plan_state.enqueue_input_sender);
                plan_state.task_handle.await??;
                let skipped = plan_state
                    .skipped_corrupt_files
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                // If the snapshot failed (e.g. pipeline died), return empty stats.
                Ok(stats
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![]))
                    .with_skipped_corrupt_files(skipped))
            }
            .boxed())
        } else {
            let stats_handle = plan_state.stats_handle.clone();
            let skipped_corrupt_files = plan_state.skipped_corrupt_files.clone();
            Ok(async move {
                let skipped = skipped_corrupt_files
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                Ok(stats_handle
                    .take_input_snapshot(input_id)
                    .await
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![]))
                    .with_skipped_corrupt_files(skipped))
            }
            .boxed())
        }
    }

    pub fn cancel_plan(&mut self, fingerprint: u64) {
        // RuntimeTask drop cancels the spawned task
        self.plans.remove(&fingerprint);
    }

    /// Evict a plan whose execution loop has already finished (its enqueue
    /// channel is closed) so a subsequent `run()` rebuilds a fresh pipeline.
    /// No-op if the plan is absent or still live — used by the enqueue retry to
    /// recover from the "plan finished mid-enqueue" race without disturbing a
    /// healthy pipeline.
    pub fn discard_finished_plan(&mut self, fingerprint: u64) {
        if let Some(plan_state) = self.plans.get(&fingerprint)
            && plan_state.enqueue_input_sender.is_closed()
        {
            self.plans.remove(&fingerprint);
        }
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = BuilderContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();

        viz_pipeline_ascii(pipeline_node.as_ref(), simple)
    }

    fn repr_mermaid(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        options: MermaidDisplayOptions,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = BuilderContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();

        let display_type = if options.simple {
            DisplayLevel::Compact
        } else {
            DisplayLevel::Default
        };
        viz_pipeline_mermaid(
            pipeline_node.as_ref(),
            display_type,
            options.bottom_up,
            options.subgraph_options,
        )
    }
}

impl Drop for NativeExecutor {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(conn) = &mut self.shuffle_server_connection {
            let _ = conn.shutdown();
        }
    }
}

pub struct ExecutionEngineResult {
    receiver: crate::channel::UnboundedReceiver<ExecutionEngineResultItem>,
}

impl ExecutionEngineResult {
    async fn next(&mut self) -> Option<ExecutionEngineResultItem> {
        self.receiver.recv().await
    }

    /// Consume all pipeline output for this input_id until EOF, returning any
    /// emitted `MicroPartition`s. `FlightPartitionRef` items are skipped (they
    /// are only relevant when shuffles are enabled). Intended for tests that
    /// exercise `NativeExecutor` end-to-end and need the pipeline to finish
    /// producing output before `try_finish` is called — mirroring what the
    /// production Python `__anext__` loop does.
    pub async fn collect_partitions_for_testing(mut self) -> Vec<MicroPartition> {
        let mut out = Vec::new();
        while let Some(item) = self.receiver.recv().await {
            if let ExecutionEngineResultItem::Partition(p) = item {
                out.push(p);
            }
        }
        out
    }
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PyResultReceiver", frozen)
)]
pub struct PyResultReceiver {
    result: Arc<tokio::sync::Mutex<Option<ExecutionEngineResult>>>,
    fingerprint: u64,
    input_id: InputId,
    executor: Arc<Mutex<NativeExecutor>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyResultReceiver {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let result = self.result.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut result = result.lock().await;
            let part = result
                .as_mut()
                .expect("PyResultReceiver.__anext__() should not be called after try_finish().")
                .next()
                .await;
            Python::attach(|py| match part {
                None => Ok(py.None()),
                Some(ExecutionEngineResultItem::Partition(partition)) => {
                    Ok(PyMicroPartition::from(partition)
                        .into_pyobject(py)?
                        .unbind()
                        .into_any())
                }
                Some(ExecutionEngineResultItem::FlightPartitionRef(partition_ref)) => {
                    Ok(PyFlightPartitionRef::from(partition_ref)
                        .into_pyobject(py)?
                        .unbind()
                        .into_any())
                }
            })
        })
    }

    fn try_finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = self.result.clone();
        let executor = self.executor.clone();
        let fingerprint = self.fingerprint;
        let input_id = self.input_id;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Take the result to drop the receiver
            let mut result = result.lock().await;
            let _ = result
                .take()
                .expect("PyResultReceiver.try_finish() should not be called more than once.");
            drop(result);

            // Delegate to NativeExecutor::try_finish
            let finish_future = executor.lock().unwrap().try_finish(fingerprint, input_id)?;
            let stats = finish_future.await?;
            Ok(PyExecutionStats::from(stats))
        })
    }
}

fn dispatch_task_start_event(subscribers: &[Arc<dyn Subscriber>], event: &Event) {
    for subscriber in subscribers {
        if let Err(e) = subscriber.on_event(event.clone()) {
            log::debug!("Failed to dispatch task start event: {}", e);
        }
    }
}
