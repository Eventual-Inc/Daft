use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::DaftResult;
use common_metrics::StatSnapshot;
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{DaftContext, Subscriber};
use daft_local_plan::{LocalPhysicalPlanRef, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    MicroPartition, MicroPartitionRef,
    partitioning::{InMemoryPartitionSetCache, MicroPartitionSet, PartitionSetCache},
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    common_scan_info::ScanTaskLikeRef,
    daft_context::python::PyDaftContext,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::{partitioning::PartitionSetRef, python::PyMicroPartition},
    daft_scan::python::pylib::PyScanTask,
    pyo3::{Bound, IntoPyObject, PyAny, PyRef, PyResult, Python, pyclass, pymethods},
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, Sender, create_channel},
    pipeline::{
        RelationshipInformation, RuntimeContext, get_pipeline_relationship_mapping,
        translate_physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid,
    },
    plan_input::{InputId, InputSender, PipelineMessage, PlanInput},
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{QueryEndState, RuntimeStatsManager},
};

/// Global tokio runtime shared by all NativeExecutor instances
static GLOBAL_RUNTIME: OnceLock<Handle> = OnceLock::new();

/// Internal state for result channels.
/// Contains the channel map and a flag to track if channels are closed.
struct ChannelsState {
    channels: HashMap<
        InputId,
        (
            Option<Sender<Arc<MicroPartition>>>,
            Option<Receiver<Arc<MicroPartition>>>,
        ),
    >,
    /// Flag to track if channels are closed (no new channels can be created).
    /// Set to true when MessageRouter is dropped.
    closed: bool,
}

/// Manages result channels for pipeline execution.
/// Provides methods to get senders and receivers, with error handling
/// if the requested item is None when the channel exists.
struct ResultChannels {
    state: Arc<Mutex<ChannelsState>>,
}

impl ResultChannels {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ChannelsState {
                channels: HashMap::new(),
                closed: false,
            })),
        }
    }

    /// Take the sender for the given input_id (removes it from the map).
    /// Creates a new channel if one doesn't exist.
    /// Returns None if the sender was already taken or if channels are closed.
    /// This is used by MessageRouter to get senders without erroring.
    fn take_sender(&self, input_id: InputId) -> Option<Sender<Arc<MicroPartition>>> {
        let mut state_guard = self.state.lock().unwrap();
        // Check if channels are closed before creating new ones
        if state_guard.closed {
            // Channels are closed, only return existing senders
            state_guard
                .channels
                .get_mut(&input_id)
                .and_then(|entry| entry.0.take())
        } else {
            let entry = state_guard.channels.entry(input_id).or_insert_with(|| {
                let (tx, rx) = create_channel(1);
                (Some(tx), Some(rx))
            });
            entry.0.take()
        }
    }

    /// Clear the sender for the given input_id (used for flush operations).
    fn clear_sender(&self, input_id: InputId) {
        let mut state_guard = self.state.lock().unwrap();
        if let Some(entry) = state_guard.channels.get_mut(&input_id) {
            entry.0.take();
        }
    }

    /// Clear all senders (used when dropping MessageRouter).
    /// Also marks channels as closed to prevent new channels from being created.
    fn clear_all_senders(&self) {
        let mut state_guard = self.state.lock().unwrap();
        // Mark channels as closed to prevent new channel creation
        state_guard.closed = true;
        for (_, (sender, _)) in state_guard.channels.iter_mut() {
            sender.take();
        }
    }

    /// Get the receiver for the given input_id.
    /// Creates a new channel if one doesn't exist.
    /// Returns None if the channel exists but the receiver is None, or if channels are closed.
    fn get_receiver(&self, input_id: InputId) -> Option<Receiver<Arc<MicroPartition>>> {
        let state_guard = self.state.lock();
        if state_guard.is_err() {
            panic!("state guard is None");
        }
        let mut state_guard = state_guard.unwrap();
        // Check if channels are closed before creating new ones
        let entry = state_guard.channels.entry(input_id);
        match entry {
            std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().1.take(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let (tx, rx) = create_channel(1);
                entry.insert((Some(tx), None));
                Some(rx)
            }
        }
    }
}

impl Clone for ResultChannels {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// Routes pipeline messages to per-input_id channels.
/// Maintains a local cache of senders to avoid mutex contention,
/// and handles cleanup of dropped receivers on drop.
struct MessageRouter {
    /// Local cache of senders to avoid locking the mutex on every message
    local_senders: HashMap<InputId, Sender<Arc<MicroPartition>>>,
    /// Track input_ids whose receivers have been dropped - ignore subsequent messages for these
    finished_inputs: HashSet<InputId>,
    /// Global shared channels map
    result_channels: ResultChannels,
}

impl MessageRouter {
    fn new(result_channels: ResultChannels) -> Self {
        Self {
            local_senders: HashMap::new(),
            finished_inputs: HashSet::new(),
            result_channels,
        }
    }

    /// Route a message to the appropriate channel based on its input_id.
    async fn route_message(&mut self, msg: PipelineMessage) {
        match msg {
            PipelineMessage::Flush(input_id) => {
                // For flush messages, immediately drop the sender from both local and global
                self.local_senders.remove(&input_id);
                self.result_channels.clear_sender(input_id);
            }
            PipelineMessage::Morsel {
                input_id,
                partition,
            } => {
                // Get sender from local cache or global channels
                let sender = match self.local_senders.remove(&input_id) {
                    Some(sender) => sender,
                    None => {
                        match self.result_channels.take_sender(input_id) {
                            Some(sender) => sender,
                            None => {
                                // Sender already taken, receiver must have been dropped
                                self.finished_inputs.insert(input_id);
                                return;
                            }
                        }
                    }
                };

                // Send the morsel
                if sender.send(partition).await.is_err() {
                    // Send failed, mark input as failed and remove sender from both local and global
                    self.finished_inputs.insert(input_id);
                    return;
                }

                // Put sender back in local cache for reuse
                self.local_senders.insert(input_id, sender);
            }
        }
    }
}

impl Drop for MessageRouter {
    fn drop(&mut self) {
        // Remove all senders from global channels
        self.result_channels.clear_all_senders();
    }
}

/// Get or initialize the global tokio runtime
#[cfg(feature = "python")]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_all();
        pyo3_async_runtimes::tokio::init(builder);
        std::thread::Builder::new()
            .name("pyo3_global_runtime".to_string())
            .spawn(move || {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(futures::future::pending::<()>());
            })
            .expect("Failed to spawn global runtime thread");
        pyo3_async_runtimes::tokio::get_runtime().handle().clone()
    })
}

#[cfg(not(feature = "python"))]
fn get_global_runtime() -> &'static Handle {
    unimplemented!("get_global_runtime is not implemented without python feature");
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "NativeExecutor", frozen)
)]
pub struct PyNativeExecutor {
    executor: NativeExecutor,
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
            executor: NativeExecutor::new(),
        }
    }

    #[pyo3(signature = (local_physical_plan, psets, daft_ctx, context=None))]
    pub fn run<'a>(
        &self,
        py: Python<'a>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        psets: HashMap<String, Vec<PyMicroPartition>>,
        daft_ctx: &PyDaftContext,
        context: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let native_psets: HashMap<String, Arc<MicroPartitionSet>> = psets
            .into_iter()
            .map(|(part_id, parts)| {
                (
                    part_id,
                    Arc::new(
                        parts
                            .into_iter()
                            .map(std::convert::Into::into)
                            .collect::<Vec<Arc<MicroPartition>>>()
                            .into(),
                    ),
                )
            })
            .collect();
        let psets = InMemoryPartitionSetCache::new(&native_psets);
        let daft_ctx: &DaftContext = daft_ctx.into();
        let res = py.detach(|| {
            self.executor.run(
                &local_physical_plan.plan,
                &psets,
                daft_ctx.execution_config(),
                daft_ctx.subscribers(),
                context,
                should_enable_explain_analyze(),
            )
        })?;

        let py_execution_result = PyExecutionEngineResult {
            result: Mutex::new(Some(res)),
        };
        Ok(py_execution_result.into_pyobject(py)?.into_any())
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

#[derive(Debug, Clone)]
pub(crate) struct NativeExecutor {
    cancel: CancellationToken,
}

impl NativeExecutor {
    pub fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
        }
    }

    pub fn run(
        &self,
        local_physical_plan: &LocalPhysicalPlanRef,
        psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        enable_explain_analyze: bool,
    ) -> DaftResult<ExecutionEngineResult> {
        let cancel = self.cancel.clone();
        let additional_context = additional_context.unwrap_or_default();
        let ctx = RuntimeContext::new_with_context(additional_context.clone());
        let (pipeline, input_senders) =
            translate_physical_plan_to_pipeline(local_physical_plan, psets, &exec_cfg, &ctx)?;

        let query_id: common_metrics::QueryID = additional_context
            .get("query_id")
            .ok_or_else(|| {
                common_error::DaftError::ValueError(
                    "query_id not found in additional_context".to_string(),
                )
            })?
            .clone()
            .into();

        // Spawn execution on the global runtime - returns immediately
        let handle = get_global_runtime();
        let stats_manager = RuntimeStatsManager::try_new(handle, &pipeline, subscribers, query_id)?;
        let result_channels = ResultChannels::new();
        let result_channels_for_task = result_channels.clone();
        let task = async move {
            let stats_manager_handle = stats_manager.handle();
            let memory_manager = get_or_init_memory_manager();
            let mut runtime_handle =
                ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
            let mut receiver = pipeline.start(&mut runtime_handle)?;

            // Message router handles routing messages to per-input_id channels
            let mut message_router = MessageRouter::new(result_channels_for_task);

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
                    Some(join_result) = runtime_handle.join_next() => {
                        match join_result {
                            Ok(Ok(())) => {
                                // Task completed successfully, continue
                            }
                            Ok(Err(e)) => {
                                // Task failed with error
                                break (Err(e.into()), QueryEndState::Failed);
                            }
                            Err(e) if !matches!(&e, crate::Error::JoinError { source } if source.is_cancelled()) => {
                                // Join error (unless cancelled)
                                break (Err(e.into()), QueryEndState::Failed);
                            }
                            _ => {
                                break (Ok(()), QueryEndState::Cancelled);
                            }
                        }
                    }
                    msg = receiver.recv() => {
                        match msg {
                            Some(msg) => {
                                message_router.route_message(msg).await;
                            }
                            None => {
                                let res = runtime_handle.shutdown().await;
                                break (res, QueryEndState::Finished);
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

        Ok(ExecutionEngineResult {
            handle: RuntimeTask::new(handle, task),
            input_senders,
            result_channels,
        })
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let (pipeline_node, _) = translate_physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
        )
        .unwrap();

        viz_pipeline_ascii(pipeline_node.as_ref(), simple)
    }

    fn repr_mermaid(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        options: MermaidDisplayOptions,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let (pipeline_node, _) = translate_physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
        )
        .unwrap();

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
        let (physical_plan, _) = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let (pipeline_node, _) = translate_physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
        )
        .unwrap();
        get_pipeline_relationship_mapping(&*pipeline_node)
    }
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

type ExecutionEngineFinalResult = DaftResult<Vec<(usize, StatSnapshot)>>;

pub(crate) struct ExecutionEngineResult {
    handle: RuntimeTask<ExecutionEngineFinalResult>,
    input_senders: HashMap<String, InputSender>,
    result_channels: ResultChannels,
}

impl ExecutionEngineResult {
    async fn finish(self) -> ExecutionEngineFinalResult {
        drop(self.input_senders);
        let result = self.handle.await;
        match result {
            Ok(Ok(final_stats)) => Ok(final_stats),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    }

    /// Get the sender for the given key
    pub fn get_input_sender(&self, key: &str) -> DaftResult<InputSender> {
        self.input_senders
            .get(key)
            .ok_or_else(|| {
                common_error::DaftError::ValueError(format!(
                    "No input sender found for key: {}",
                    key
                ))
            })
            .map(|s| s.clone())
    }
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PyExecutionEngineResult", frozen)
)]
pub struct PyExecutionEngineResult {
    result: Mutex<Option<ExecutionEngineResult>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyExecutionEngineResult {
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = py.detach(|| {
            let mut result_guard = self.result.lock().unwrap();
            match result_guard.take() {
                Some(result) => Ok(result),
                None => Err(pyo3::exceptions::PyValueError::new_err(
                    "ExecutionEngineResult has already been finished".to_string(),
                )),
            }
        })?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = result.finish().await?;
            Ok(bincode::encode_to_vec(&stats, bincode::config::legacy())
                .expect("Failed to serialize stats object"))
        })
    }

    /// Enqueue inputs across multiple source_ids for a single input_id and return the result receiver.
    /// If no inputs are provided (all None), this will just return the receiver for the input_id.
    /// This consolidates enqueue_scan_tasks, enqueue_in_memory, and enqueue_glob_paths into a single method.
    fn enqueue_inputs<'py>(
        &self,
        py: Python<'py>,
        input_id: u32,
        scan_tasks: Option<HashMap<String, Vec<PyScanTask>>>,
        in_memory: Option<HashMap<String, Vec<PyMicroPartition>>>,
        glob_paths: Option<HashMap<String, Vec<String>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Check if we have any inputs to enqueue
        let has_inputs = scan_tasks.is_some() || in_memory.is_some() || glob_paths.is_some();

        // Get all senders and result_channels while holding the lock
        let (senders, result_channels): (HashMap<String, DaftResult<InputSender>>, ResultChannels) =
            py.detach(
                || -> PyResult<(HashMap<String, DaftResult<InputSender>>, ResultChannels)> {
                    let result_guard = self.result.lock().unwrap();
                    let exec_result = match result_guard.as_ref() {
                        Some(result) => result,
                        None => {
                            return Ok((HashMap::new(), ResultChannels::new()));
                        }
                    };

                    let mut senders: HashMap<String, DaftResult<InputSender>> = HashMap::new();

                    if has_inputs {
                        // Collect all source_ids from all input types
                        let mut all_keys = HashSet::new();
                        if let Some(ref scan_tasks) = scan_tasks {
                            all_keys.extend(scan_tasks.keys());
                        }
                        if let Some(ref in_memory) = in_memory {
                            all_keys.extend(in_memory.keys());
                        }
                        if let Some(ref glob_paths) = glob_paths {
                            all_keys.extend(glob_paths.keys());
                        }

                        // Get senders for all source_ids
                        for key in all_keys {
                            let sender = exec_result.get_input_sender(key);
                            senders.insert(key.clone(), sender);
                        }
                    }

                    Ok((senders, exec_result.result_channels.clone()))
                },
            )?;

        // Prepare plan inputs grouped by source_id
        let mut plan_inputs_by_key: Vec<(String, InputId, PlanInput)> = Vec::new();

        if let Some(tasks_map) = scan_tasks {
            for (key, tasks) in tasks_map {
                let scan_tasks: Vec<ScanTaskLikeRef> = tasks
                    .into_iter()
                    .map(|pytask| pytask.0 as Arc<dyn common_scan_info::ScanTaskLike>)
                    .collect();
                plan_inputs_by_key.push((key, input_id, PlanInput::ScanTasks(scan_tasks)));
            }
        }

        if let Some(partitions_map) = in_memory {
            for (key, partitions) in partitions_map {
                let micro_partitions: Vec<MicroPartitionRef> =
                    partitions.into_iter().map(|pymp| pymp.into()).collect();
                let partition_set: PartitionSetRef<MicroPartitionRef> =
                    Arc::new(MicroPartitionSet::from(micro_partitions));
                plan_inputs_by_key.push((
                    key,
                    input_id,
                    PlanInput::InMemoryPartitions(partition_set),
                ));
            }
        }

        if let Some(paths_map) = glob_paths {
            for (key, paths) in paths_map {
                plan_inputs_by_key.push((key, input_id, PlanInput::GlobPaths(paths)));
            }
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Enqueue inputs if provided
            for (key, input_id, plan_input) in plan_inputs_by_key {
                if let Some(sender_result) = senders.get(&key) {
                    match sender_result {
                        Ok(sender) => {
                            // If send fails (e.g., channel closed due to cancellation), return gracefully
                            let _ = sender.send(input_id, plan_input).await;
                        }
                        Err(_) => {
                            // Sender lookup failed, skip this input
                        }
                    }
                }
            }

            // Get and return the receiver for this input_id
            let receiver = match result_channels.get_receiver(input_id) {
                Some(receiver) => receiver,
                None => return Ok(None),
            };
            Ok(Some(PyResultReceiver {
                receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
                input_id: input_id,
            }))
        })
    }
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PyResultReceiver", frozen)
)]
pub struct PyResultReceiver {
    receiver: Arc<tokio::sync::Mutex<Receiver<Arc<MicroPartition>>>>,
    #[allow(dead_code)]
    input_id: InputId,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyResultReceiver {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let receiver = self.receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut receiver = receiver.lock().await;
            let partition = receiver.recv().await;
            Ok(partition.map(|p| PyMicroPartition::from(p)))
        })
    }
}
