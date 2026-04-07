use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::DaftResult;
use common_metrics::{QueryEndState, QueryID};
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{DaftContext, Subscriber};
use daft_local_plan::{ExecutionStats, Input, InputId, LocalPhysicalPlanRef, SourceId, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
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
    pyo3::{
        Bound, IntoPyObject, PyAny, PyRef, PyResult, Python, pyclass, pymethods, sync::MutexExt,
    },
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Sender, UnboundedSender, create_channel, create_unbounded_channel},
    pipeline::{
        BuilderContext, PipelineMessage, translate_physical_plan_to_pipeline, viz_pipeline_ascii,
        viz_pipeline_mermaid,
    },
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{RuntimeStatsManager, RuntimeStatsManagerHandle},
    shuffle_metadata::ShuffleMetadata,
};

enum ExecutionEngineResultItem {
    Partition(MicroPartition),
    ShuffleMetadata(ShuffleMetadata),
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
    unimplemented!("get_global_runtime is not implemented without python feature");
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
            PipelineMessage::ShuffleMetadata { input_id, metadata } => {
                if let Some(sender) = self.output_senders.get(&input_id) {
                    let _ = sender.send(ExecutionEngineResultItem::ShuffleMetadata(metadata));
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
        let (fingerprint, enqueue_future) = {
            self.executor.lock_py_attached(py).unwrap().run(
                &plan,
                exec_cfg,
                subscribers,
                context,
                inputs,
                input_id,
                maintain_order,
            )?
        };

        let executor = self.executor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = enqueue_future.await?;
            Ok(PyResultReceiver {
                result: Arc::new(tokio::sync::Mutex::new(Some(result))),
                fingerprint,
                input_id,
                executor,
            })
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

    pub fn clear_flight_shuffles(&self, py: Python<'_>, shuffle_ids: Vec<u64>) -> PyResult<usize> {
        Ok(self
            .executor
            .lock_py_attached(py)
            .unwrap()
            .clear_flight_shuffles(&shuffle_ids))
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

fn parse_context(ctx: Option<&HashMap<String, String>>) -> (QueryID, u64) {
    let query_id = ctx
        .as_ref()
        .and_then(|c| c.get("query_id"))
        .map(|s| QueryID::from(s.as_str()))
        .unwrap_or_else(|| QueryID::from(""));
    let fingerprint = ctx
        .as_ref()
        .and_then(|c| c.get("plan_fingerprint"))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    (query_id, fingerprint)
}

/// The core execution loop that drives a pipeline to completion.
/// Receives inputs via `enqueue_input_rx`, routes pipeline outputs to
/// per-input_id channels, and runs until the pipeline finishes, errors,
/// or is cancelled.
async fn run_execution_loop(
    cancel: CancellationToken,
    stats_manager: RuntimeStatsManager,
    mut enqueue_input_rx: crate::channel::Receiver<EnqueueInputMessage>,
    input_senders: Arc<HashMap<SourceId, crate::input_sender::InputSender>>,
    pipeline: Box<dyn crate::pipeline::PipelineNode>,
    maintain_order: bool,
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
                        let status = if res.is_ok() { QueryEndState::Finished } else { QueryEndState::Failed };
                        break (res, status);
                    }
                }
            }
        }
    };

    stats_manager.finish(finish_status).await;
    flush_opentelemetry_providers();
    result
}

pub(crate) struct NativeExecutor {
    cancel: CancellationToken,
    is_flotilla_worker: bool,
    shuffle_server: Option<Arc<ShuffleFlightServer>>,
    shuffle_server_connection: Option<FlightServerConnectionHandle>,
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
                plans: HashMap::new(),
            }
        } else {
            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: false,
                shuffle_server: None,
                shuffle_server_connection: None,
                plans: HashMap::new(),
            }
        }
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.shuffle_server_connection
            .as_ref()
            .map(|conn| conn.shuffle_address())
    }

    pub fn clear_flight_shuffles(&self, shuffle_ids: &[u64]) -> usize {
        let Some(shuffle_server) = &self.shuffle_server else {
            return 0;
        };
        common_runtime::get_io_runtime(true)
            .block_on_current_thread(shuffle_server.clear_shuffles(shuffle_ids))
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
    ) -> DaftResult<(u64, BoxFuture<'static, DaftResult<ExecutionEngineResult>>)> {
        let (query_id, fingerprint) = parse_context(additional_context.as_ref());

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
            let task = run_execution_loop(
                cancel,
                stats_manager,
                enqueue_input_rx,
                input_senders,
                pipeline,
                maintain_order,
            );

            let task_handle = RuntimeTask::new(handle, task);
            self.plans.insert(
                fingerprint,
                PlanState {
                    task_handle,
                    enqueue_input_sender: enqueue_input_tx,
                    stats_handle,
                    active_input_ids: HashSet::new(),
                },
            );
        }

        let plan_state = self.plans.get_mut(&fingerprint).unwrap();
        let enqueue_input_sender = plan_state.enqueue_input_sender.clone();
        plan_state.active_input_ids.insert(input_id);

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
                    return Err(common_error::DaftError::InternalError(
                        "Plan execution task has died; cannot enqueue new input".to_string(),
                    ));
                }
                Ok(ExecutionEngineResult {
                    receiver: result_rx,
                    shuffle_metadata: None,
                })
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
                // If the snapshot failed (e.g. pipeline died), return empty stats.
                Ok(stats.unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![])))
            }
            .boxed())
        } else {
            let stats_handle = plan_state.stats_handle.clone();
            Ok(async move {
                Ok(stats_handle
                    .take_input_snapshot(input_id)
                    .await
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![])))
            }
            .boxed())
        }
    }

    pub fn cancel_plan(&mut self, fingerprint: u64) {
        // RuntimeTask drop cancels the spawned task
        self.plans.remove(&fingerprint);
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
    shuffle_metadata: Option<ShuffleMetadata>,
}

impl ExecutionEngineResult {
    async fn next(&mut self) -> Option<MicroPartition> {
        while let Some(item) = self.receiver.recv().await {
            match item {
                ExecutionEngineResultItem::Partition(partition) => return Some(partition),
                ExecutionEngineResultItem::ShuffleMetadata(metadata) => {
                    self.shuffle_metadata = Some(metadata);
                }
            }
        }
        None
    }

    async fn into_shuffle_metadata(mut self) -> Option<ShuffleMetadata> {
        while let Some(item) = self.receiver.recv().await {
            if let ExecutionEngineResultItem::ShuffleMetadata(metadata) = item {
                self.shuffle_metadata = Some(metadata);
            }
        }
        self.shuffle_metadata
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
            Ok(part.map(PyMicroPartition::from))
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

    fn try_finish_with_shuffle_metadata<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = self.result.clone();
        let executor = self.executor.clone();
        let fingerprint = self.fingerprint;
        let input_id = self.input_id;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut result_guard = result.lock().await;
            let execution_result = result_guard
                .take()
                .expect("PyResultReceiver.try_finish_with_shuffle_metadata() should not be called more than once.");
            drop(result_guard);

            let shuffle_metadata = execution_result.into_shuffle_metadata().await;
            let finish_future = executor.lock().unwrap().try_finish(fingerprint, input_id)?;
            let stats = finish_future.await?;
            Python::attach(|py| {
                let py_metadata = shuffle_metadata
                    .as_ref()
                    .map(|metadata| metadata.to_pyobject(py))
                    .transpose()?;
                Ok((PyExecutionStats::from(stats), py_metadata)
                    .into_pyobject(py)?
                    .unbind())
            })
        })
    }
}
