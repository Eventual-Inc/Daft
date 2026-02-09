use std::{
    collections::{HashMap, HashSet},
    fs::File,
    hash::{Hash, Hasher},
    io::Write,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::DaftResult;
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{DaftContext, Subscriber};
use daft_local_plan::{
    ExecutionEngineFinalResult, Input, InputId, LocalPhysicalPlanRef, SourceId, translate,
};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use futures::{FutureExt, Stream, future::BoxFuture};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_local_plan::python::PyExecutionEngineFinalResult,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    pyo3::{Bound, PyAny, PyRef, PyResult, Python, pyclass, pymethods, sync::MutexExt},
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, Sender, create_channel},
    pipeline::{
        RelationshipInformation, RuntimeContext, get_pipeline_relationship_mapping,
        translate_physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid,
    },
    pipeline_message::PipelineMessage,
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{QueryEndState, RuntimeStatsManager, RuntimeStatsSnapshot},
};

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
    result_sender: Sender<Arc<MicroPartition>>,
}

/// Routes pipeline messages to per-input_id channels.
struct MessageRouter {
    output_senders: HashMap<InputId, Sender<Arc<MicroPartition>>>,
}

impl MessageRouter {
    fn new() -> Self {
        Self {
            output_senders: HashMap::new(),
        }
    }

    /// Route a message to the appropriate channel based on its input_id.
    async fn route_message(&mut self, msg: PipelineMessage) {
        match msg {
            PipelineMessage::Flush(input_id) => {
                self.output_senders.remove(&input_id);
            }
            PipelineMessage::Morsel {
                input_id,
                partition,
            } => {
                if let Some(sender) = self.output_senders.get(&input_id) {
                    let _ = sender.send(partition).await;
                }
            }
        }
    }

    fn insert_output_sender(&mut self, input_id: InputId, sender: Sender<Arc<MicroPartition>>) {
        self.output_senders.insert(input_id, sender);
    }
}

/// Per-plan execution state
struct PlanState {
    task_handle: RuntimeTask<DaftResult<ExecutionEngineFinalResult>>,
    enqueue_input_sender: Sender<EnqueueInputMessage>,
    stats_snapshot: RuntimeStatsSnapshot,
    active_input_ids: HashSet<InputId>,
}

struct ActivePlansRegistry {
    plans: HashMap<u64, PlanState>,
}

impl ActivePlansRegistry {
    fn new() -> Self {
        Self {
            plans: HashMap::new(),
        }
    }

    fn get_or_create_plan<F>(&mut self, fingerprint: u64, plan_factory: F) -> DaftResult<()>
    where
        F: FnOnce() -> DaftResult<(
            RuntimeTask<DaftResult<ExecutionEngineFinalResult>>,
            Sender<EnqueueInputMessage>,
            RuntimeStatsSnapshot,
        )>,
    {
        if self.plans.contains_key(&fingerprint) {
            return Ok(());
        }
        let (task_handle, enqueue_sender, stats_snapshot) = plan_factory()?;
        let state = PlanState {
            task_handle,
            enqueue_input_sender: enqueue_sender,
            stats_snapshot,
            active_input_ids: HashSet::new(),
        };
        self.plans.insert(fingerprint, state);
        Ok(())
    }

    fn cancel_plan(&mut self, fingerprint: u64) {
        // RuntimeTask drop cancels the spawned task
        self.plans.remove(&fingerprint);
    }
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "NativeExecutor", frozen)
)]
pub struct PyNativeExecutor {
    executor: Arc<Mutex<NativeExecutor>>,
}

#[cfg(feature = "python")]
impl Default for PyNativeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PyNativeExecutor {
    #[new]
    pub fn new() -> Self {
        Self {
            executor: Arc::new(Mutex::new(NativeExecutor::new())),
        }
    }

    #[pyo3(signature = (local_physical_plan, daft_ctx, inputs, input_id, context=None))]
    pub fn run<'py>(
        &self,
        py: Python<'py>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        daft_ctx: &PyDaftContext,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
        context: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let daft_ctx: &DaftContext = daft_ctx.into();
        let plan = local_physical_plan.plan.clone();
        let query_id = context
            .as_ref()
            .and_then(|c| c.get("query_id"))
            .map(|s| s.as_str())
            .unwrap_or("");
        let fingerprint = plan_key(plan.fingerprint(), query_id);
        let exec_cfg = daft_ctx.execution_config();
        let subscribers = daft_ctx.subscribers();
        let enqueue_future = {
            self.executor.lock_py_attached(py).unwrap().run(
                &plan,
                exec_cfg,
                subscribers,
                context,
                inputs,
                input_id,
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
        self.executor
            .lock_py_attached(py)
            .unwrap()
            .active_plans
            .plans
            .len()
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

    #[staticmethod]
    pub fn get_relationship_info(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<RelationshipInformation> {
        Ok(NativeExecutor::get_relationship_info(
            &logical_plan_builder.builder,
            cfg.config,
        ))
    }
}

pub(crate) struct NativeExecutor {
    cancel: CancellationToken,
    active_plans: ActivePlansRegistry,
}

impl NativeExecutor {
    pub fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            active_plans: ActivePlansRegistry::new(),
        }
    }

    pub fn run(
        &mut self,
        local_physical_plan: &LocalPhysicalPlanRef,
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
    ) -> DaftResult<BoxFuture<'static, DaftResult<ExecutionEngineResult>>> {
        let query_id_str = additional_context
            .as_ref()
            .and_then(|c| c.get("query_id"))
            .map(|s| s.as_str())
            .unwrap_or("");
        let fingerprint = plan_key(local_physical_plan.fingerprint(), query_id_str);
        let enable_explain_analyze = should_enable_explain_analyze();

        // Get or create plan handle from registry
        self.active_plans.get_or_create_plan(fingerprint, || {
            let cancel = self.cancel.clone();
            let additional_context = additional_context.clone().unwrap_or_default();
            let ctx = RuntimeContext::new_with_context(additional_context.clone());
            let (mut pipeline, input_senders) =
                translate_physical_plan_to_pipeline(local_physical_plan, &exec_cfg, &ctx)?;

            let query_id: common_metrics::QueryID = additional_context
                .get("query_id")
                .ok_or_else(|| {
                    common_error::DaftError::ValueError(
                        "query_id not found in additional_context".to_string(),
                    )
                })?
                .clone()
                .into();

            let handle = get_global_runtime();
            let stats_manager =
                RuntimeStatsManager::try_new(handle, &pipeline, subscribers, query_id)?;
            let stats_snapshot = stats_manager.snapshot_handle();

            let (enqueue_input_tx, mut enqueue_input_rx) =
                create_channel::<EnqueueInputMessage>(1);

            let input_senders = Arc::new(input_senders);

            let task = async move {
                let stats_manager_handle = stats_manager.handle();
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle =
                    ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
                let mut receiver = pipeline.start(true, &mut runtime_handle)?;

                let mut message_router = MessageRouter::new();
                let mut input_exhausted = false;
                let mut pipeline_finished = false;
                let mut shutdown_result: Option<(DaftResult<()>, QueryEndState)> = None;
                let mut input_senders = Some(input_senders);

                let (result, finish_status) = loop {
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {
                            log::info!("Execution engine cancelled");
                            break (Ok(()), QueryEndState::Cancelled);
                        }
                        _ = tokio::signal::ctrl_c() => {
                            log::info!("Received Ctrl-C, shutting down execution engine");
                            break (Ok(()), QueryEndState::Cancelled);
                        }
                        Some(join_result) = runtime_handle.join_next(), if !pipeline_finished => {
                            match join_result {
                                Ok(()) => {
                                    // Task completed successfully, continue
                                }
                                Err(e) => {
                                    if matches!(&e, common_error::DaftError::JoinError(source) if source.is_cancelled()) {
                                        break (Ok(()), QueryEndState::Cancelled);
                                    }
                                    break (Err(e), QueryEndState::Failed);
                                }
                            }
                        }
                        enqueue_msg = enqueue_input_rx.recv(), if !input_exhausted => {
                            match enqueue_msg {
                                Some(EnqueueInputMessage { input_id, inputs, result_sender }) => {
                                    if pipeline_finished {
                                        drop(result_sender);
                                    } else {
                                        message_router.insert_output_sender(input_id, result_sender);
                                        // Send inputs to pipeline sources. Unbounded channels
                                        // ensure sends never block the select loop.
                                        let senders = input_senders.as_ref().unwrap();
                                        let provided_keys: HashSet<SourceId> =
                                            inputs.keys().copied().collect();
                                        for (key, plan_input) in inputs {
                                            if let Some(sender) = senders.get(&key) {
                                                let _ = sender.send(input_id, plan_input);
                                            }
                                        }
                                        // Send empty inputs for any source that didn't receive data,
                                        // so the pipeline can still flush this input_id properly.
                                        for (key, sender) in senders.iter() {
                                            if !provided_keys.contains(key) {
                                                let _ = sender.send_empty(input_id);
                                            }
                                        }
                                    }
                                }
                                None => {
                                    // All senders dropped — no more inputs coming.
                                    // Drop our Arc ref; input channels close once
                                    // in-flight dispatch tasks finish.
                                    input_senders.take();
                                    input_exhausted = true;
                                    if pipeline_finished {
                                        break shutdown_result.take().unwrap();
                                    }
                                }
                            }
                        }
                        msg = receiver.recv(), if !pipeline_finished => {
                            match msg {
                                Some(msg) => {
                                    message_router.route_message(msg).await;
                                }
                                None => {
                                    // Close all in-flight result channels immediately
                                    // so waiters get None from next() and can call
                                    // try_finish(). Must happen before shutdown() so
                                    // callers don't block waiting for results while
                                    // we wait for tasks to drain.
                                    message_router = MessageRouter::new();
                                    let res = runtime_handle.shutdown().await;
                                    pipeline_finished = true;
                                    let status = if res.is_ok() { QueryEndState::Finished } else { QueryEndState::Failed };
                                    shutdown_result = Some((res, status));
                                    if input_exhausted {
                                        break shutdown_result.take().unwrap();
                                    }
                                }
                            }
                        }
                    }
                };

                drop(message_router);

                // Finish the stats manager
                let final_stats = stats_manager.finish(finish_status).await;

                // TODO: Move into a runtime stats subscriber
                if enable_explain_analyze {
                    let curr_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis();
                    let file_name = format!("explain-analyze-{curr_ms}-mermaid.md");
                    let mut file = File::create(file_name)?;
                    writeln!(
                        file,
                        "```mermaid\n{}\n```",
                        viz_pipeline_mermaid(
                            pipeline.as_ref(),
                            DisplayLevel::Verbose,
                            true,
                            Default::default()
                        )
                    )?;
                }
                flush_opentelemetry_providers();
                result.map(|()| final_stats)
            };

            let handle = get_global_runtime();
            let task_handle = RuntimeTask::new(handle, task);
            Ok((task_handle, enqueue_input_tx, stats_snapshot))
        })?;

        // Get the enqueue sender from the plan state
        let plan_state = self.active_plans.plans.get_mut(&fingerprint).unwrap();
        let enqueue_input_sender = plan_state.enqueue_input_sender.clone();
        plan_state.active_input_ids.insert(input_id);

        Ok(async move {
            let (result_tx, result_rx) = create_channel(1);

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
            })
        }
        .boxed())
    }

    /// Finish tracking an input_id. If no active input_ids remain (or the
    /// enqueue channel is closed), removes the plan from the registry and
    /// awaits the exec task for its final result.
    pub fn try_finish(
        &mut self,
        fingerprint: u64,
        input_id: InputId,
    ) -> DaftResult<BoxFuture<'static, DaftResult<ExecutionEngineFinalResult>>> {
        let should_remove = if let Some(plan_state) = self.active_plans.plans.get_mut(&fingerprint)
        {
            plan_state.active_input_ids.remove(&input_id);
            let is_empty = plan_state.active_input_ids.is_empty();
            let is_closed = plan_state.enqueue_input_sender.is_closed();
            is_empty || is_closed
        } else {
            return Err(common_error::DaftError::InternalError(format!(
                "try_finish called for unknown plan fingerprint {fingerprint}"
            )));
        };

        if should_remove {
            let plan_state = self.active_plans.plans.remove(&fingerprint).unwrap();
            let enqueue_input_sender = plan_state.enqueue_input_sender;
            let task_handle = plan_state.task_handle;
            Ok(async move {
                // Drop the sender so the exec task sees input exhaustion
                drop(enqueue_input_sender);
                // Await the exec task for its final result (includes stats)
                let result = task_handle.await?;
                result
            }
            .boxed())
        } else {
            let stats_snapshot = self
                .active_plans
                .plans
                .get(&fingerprint)
                .unwrap()
                .stats_snapshot
                .clone();
            Ok(async move { Ok(stats_snapshot.snapshot()) }.boxed())
        }
    }

    pub fn cancel_plan(&mut self, fingerprint: u64) {
        self.active_plans.cancel_plan(fingerprint);
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = RuntimeContext::new();
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
        let ctx = RuntimeContext::new();
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
    fn get_relationship_info(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
    ) -> RelationshipInformation {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = RuntimeContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();
        get_pipeline_relationship_mapping(&*pipeline_node)
    }
}

impl Drop for NativeExecutor {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Combine a plan fingerprint with a query_id to produce a plan key.
/// Plans are only reused when both the plan shape AND query match.
fn plan_key(plan_fingerprint: u64, query_id: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    plan_fingerprint.hash(&mut hasher);
    query_id.hash(&mut hasher);
    hasher.finish()
}

fn should_enable_explain_analyze() -> bool {
    let explain_var_name = "DAFT_DEV_ENABLE_EXPLAIN_ANALYZE";
    if let Ok(val) = std::env::var(explain_var_name)
        && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    {
        true
    } else {
        false
    }
}

pub struct ExecutionEngineResult {
    receiver: Receiver<Arc<MicroPartition>>,
}

impl ExecutionEngineResult {
    async fn next(&mut self) -> Option<Arc<MicroPartition>> {
        self.receiver.recv().await
    }

    // Should be used independently of next() and try_finish()
    pub fn into_stream(self) -> impl Stream<Item = Arc<MicroPartition>> {
        struct StreamState {
            receiver: Receiver<Arc<MicroPartition>>,
        }

        let state = StreamState {
            receiver: self.receiver,
        };

        futures::stream::unfold(state, |mut state| async {
            state
                .receiver
                .recv()
                .await
                .map(|partition| (partition, state))
        })
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
            Ok(PyExecutionEngineFinalResult::from(stats))
        })
    }
}
