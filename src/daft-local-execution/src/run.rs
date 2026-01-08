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
    partitioning::MicroPartitionSet,
};
use futures::{future::BoxFuture, FutureExt};
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
    pyo3::{sync::MutexExt, Bound, PyAny, PyRef, PyResult, Python, pyclass, pymethods},
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

/// Message sent to the execution task to enqueue inputs
pub(crate) struct EnqueueInputMessage {
    /// The input_id for this enqueue operation
    input_id: InputId,
    /// Plan inputs grouped by source_id (key, input_id, PlanInput)
    plan_inputs_by_key: Vec<(String, InputId, PlanInput)>,
    /// Sender for results of this input_id
    result_sender: Sender<Arc<MicroPartition>>,
}

/// Global tokio runtime shared by all NativeExecutor instances
static GLOBAL_RUNTIME: OnceLock<Handle> = OnceLock::new();


/// Routes pipeline messages to per-input_id channels.
/// Uses local output_senders map maintained by the execution task.
struct MessageRouter {
    /// Local cache of output senders for each input_id
    output_senders: HashMap<InputId, Sender<Arc<MicroPartition>>>,
}

impl MessageRouter {
    fn new(output_senders: HashMap<InputId, Sender<Arc<MicroPartition>>>) -> Self {
        Self {
            output_senders,
        }
    }

    /// Route a message to the appropriate channel based on its input_id.
    async fn route_message(&mut self, msg: PipelineMessage) {
        match msg {
            PipelineMessage::Flush(input_id) => {
                // For flush messages, remove the sender from local map
                self.output_senders.remove(&input_id);
            }
            PipelineMessage::Morsel {
                input_id,
                partition,
            } => {
                // Get sender from local map
                if let Some(sender) = self.output_senders.get(&input_id) {
                    // Send the morsel
                    let _ = sender.send(partition).await;
                }
            }
        }
    }

    /// Get a mutable reference to output_senders for updating from enqueue messages
    fn insert_output_sender(&mut self, input_id: InputId, sender: Sender<Arc<MicroPartition>>) {
        self.output_senders.insert(input_id, sender);
    }
}

/// Shared state for an active plan execution
struct PlanState {
    handle: RuntimeTask<ExecutionEngineFinalResult>,
    /// Sender for enqueue_input messages to the execution task
    enqueue_input_sender: Sender<EnqueueInputMessage>,
    /// Track active input_ids for this plan
    active_input_ids: HashSet<InputId>,
}

impl PlanState {
    fn new(handle: RuntimeTask<ExecutionEngineFinalResult>, enqueue_input_sender: Sender<EnqueueInputMessage>) -> Self {
        Self {
            handle,
            enqueue_input_sender,
            active_input_ids: HashSet::new(),
        }
    }
}

/// Global registry of active plans per NativeExecutor instance
struct ActivePlansRegistry {
    plans: HashMap<u64, PlanState>,
}

impl ActivePlansRegistry {
    fn new() -> Self {
        Self {
            plans: HashMap::new(),
        }
    }

    /// Get or create a plan entry. Returns (handle, is_new).
    fn get_or_create_plan<F>(
        &mut self,
        fingerprint: u64,
        plan_factory: F,
    ) -> DaftResult<()>
    where
        F: FnOnce() -> DaftResult<(
            RuntimeTask<ExecutionEngineFinalResult>,
            Sender<EnqueueInputMessage>,
        )>,
    {
        if self.plans.contains_key(&fingerprint) {
            // Plan already exists, reuse it
            Ok(())
        } else {
            // Create new plan execution
            let (handle, enqueue_input_sender) = plan_factory()?;

            let state = PlanState::new(handle, enqueue_input_sender);
            self.plans.insert(fingerprint, state);
            Ok(())
        }
    }

    /// Cancel a plan by fingerprint
    fn cancel_plan(&mut self, fingerprint: u64) -> DaftResult<()> {
        let _ = self.plans.remove(&fingerprint);
        Ok(())
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

    /// Finish execution for a plan by fingerprint
    pub fn finish<'py>(&self, py: Python<'py>, fingerprint: u64) -> PyResult<Bound<'py, PyAny>> {


        let finish_future = self.executor.lock_py_attached(py).unwrap().finish(fingerprint)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = finish_future.await?;
            Ok(bincode::encode_to_vec(&stats, bincode::config::legacy())
                .expect("Failed to serialize stats object"))
        })
    }

    /// Mark an input as complete and check for errors. Removes the input_id from active inputs.
    /// If this was the last active input, removes the plan from active plans and finishes it.
    pub fn mark_input_complete<'py>(&self, py: Python<'py>, fingerprint: u64, input_id: u32) -> PyResult<Bound<'py, PyAny>> {
        let mark_complete_future = self.executor.lock_py_attached(py).unwrap().mark_input_complete(fingerprint, input_id)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            mark_complete_future.await?;
            Ok(())
        })
    }

    /// Run a plan (creating it if it doesn't exist) and enqueue inputs, returning the result receiver.
    /// This combines the logic of run() and enqueue_inputs() into a single operation.
    #[pyo3(signature = (local_physical_plan, daft_ctx, input_id, context=None, scan_tasks=None, in_memory=None, glob_paths=None))]
    pub fn run<'py>(
        &self,
        py: Python<'py>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        daft_ctx: &PyDaftContext,
        input_id: u32,
        context: Option<HashMap<String, String>>,
        scan_tasks: Option<HashMap<String, Vec<PyScanTask>>>,
        in_memory: Option<HashMap<String, Vec<PyMicroPartition>>>,
        glob_paths: Option<HashMap<String, Vec<String>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let daft_ctx: &DaftContext = daft_ctx.into();
        let plan = local_physical_plan.plan.clone();
        let exec_cfg = daft_ctx.execution_config();
        let subscribers = daft_ctx.subscribers();

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

        let enqueue_future = {
            // Lock the executor and call run
            let mut exec = self.executor.lock_py_attached(py).unwrap();
            exec.run(
                &plan,
                exec_cfg,
                subscribers,
                context,
                should_enable_explain_analyze(),
                input_id,
                plan_inputs_by_key,
            )?
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = enqueue_future.await?;
            Ok(result.map(|(input_id, receiver)| PyResultReceiver {
                receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
                input_id,
            }))
        })
    }

    /// Enqueue inputs across multiple source_ids for a single input_id and return the result receiver.
    /// If no inputs are provided (all None), this will just return the receiver for the input_id.
    /// This consolidates enqueue_scan_tasks, enqueue_in_memory, and enqueue_glob_paths into a single method.
    pub fn enqueue_inputs<'py>(
        &self,
        py: Python<'py>,
        fingerprint: u64,
        input_id: u32,
        scan_tasks: Option<HashMap<String, Vec<PyScanTask>>>,
        in_memory: Option<HashMap<String, Vec<PyMicroPartition>>>,
        glob_paths: Option<HashMap<String, Vec<String>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
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

        let enqueue_future = {
            // Lock the executor and call enqueue_input
            let mut exec = self.executor.lock_py_attached(py).unwrap();
            exec.enqueue_input(fingerprint, input_id, plan_inputs_by_key)?
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = enqueue_future.await?;
            Ok(result.map(|(input_id, receiver)| PyResultReceiver {
                receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
                input_id,
            }))
        })
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

    /// Cancel a plan by fingerprint
    /// This will trigger cancellation and remove the plan from active tracking
    pub fn cancel_plan<'py>(&self, py: Python<'py>, fingerprint: u64) -> PyResult<()> {
        let mut executor = self.executor.lock_py_attached(py).unwrap();
        executor.active_plans.cancel_plan(fingerprint)?;
        Ok(())
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



    /// Finish execution for a plan by fingerprint
    pub fn finish(&mut self, fingerprint: u64) -> DaftResult<BoxFuture<'static, ExecutionEngineFinalResult>> {
        if let Some(plan_state) = self.active_plans.plans.remove(&fingerprint) {
            Ok(async move {
                drop(plan_state.enqueue_input_sender);
                // We got the handle, await it
                // handle.await returns DaftResult<ExecutionEngineFinalResult>, so we need to flatten it
                match plan_state.handle.await {
                    Ok(Ok(inner_result)) => Ok(inner_result),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(e.into()),
                }
            }.boxed())
        } else {
            Err(common_error::DaftError::ValueError(format!("Plan with fingerprint {} not found or already finished", fingerprint)))
        }
    }

    /// Mark an input as complete and check for errors. Removes the input_id from active inputs.
    /// If this was the last active input, removes the plan from active plans and returns a future
    /// that finishes the plan (drops senders and awaits the handle).
    pub fn mark_input_complete(&mut self, fingerprint: u64, input_id: InputId) -> DaftResult<BoxFuture<'static, DaftResult<()>>> {
        let should_remove_plan = if let Some(plan_state) = self.active_plans.plans.get_mut(&fingerprint) {
            // Remove input_id from active_input_ids
            plan_state.active_input_ids.remove(&input_id);
            plan_state.active_input_ids.is_empty()
        } else {
            // Plan not found, nothing to do
            return Ok(async move {
                Ok(())
            }.boxed());
        };

        // If no active inputs remain, remove the plan and return a future that finishes it
        if should_remove_plan {
            if let Some(plan_state) = self.active_plans.plans.remove(&fingerprint) {
                return Ok(async move {
                    drop(plan_state.enqueue_input_sender);
                    // We got the handle, await it
                    // handle.await returns DaftResult<ExecutionEngineFinalResult>, so we need to flatten it
                    match plan_state.handle.await {
                        Ok(Ok(_inner_result)) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(e.into()),
                    }
                }.boxed());
            }
        }

        // Plan still has active inputs, nothing to do
        Ok(async move {
            Ok(())
        }.boxed())
    }

    /// Run a plan (creating it if it doesn't exist) and enqueue inputs, returning the result receiver.
    /// This combines the logic of run() and enqueue_input() into a single operation.
    pub fn run(
        &mut self,
        local_physical_plan: &LocalPhysicalPlanRef,
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        enable_explain_analyze: bool,
        input_id: InputId,
        plan_inputs_by_key: Vec<(String, InputId, PlanInput)>,
    ) -> DaftResult<BoxFuture<'static, DaftResult<Option<(InputId, Receiver<Arc<MicroPartition>>)>>>> {
        // Compute plan fingerprint for deduplication
        let fingerprint = local_physical_plan.fingerprint();

        // Get or create plan handle from registry
        self.active_plans.get_or_create_plan(fingerprint, || {
            // Create new execution
            let cancel = self.cancel.clone();
            let additional_context = additional_context.clone().unwrap_or_default();
            let ctx = RuntimeContext::new_with_context(additional_context.clone());
            let (pipeline, mut input_senders): (_, HashMap<String, InputSender>) =
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

            // Spawn execution on the global runtime - returns immediately
            let handle = get_global_runtime();
            let stats_manager = RuntimeStatsManager::try_new(handle, &pipeline, subscribers, query_id)?;

            // Create channel for enqueue_input messages
            let (enqueue_input_tx, mut enqueue_input_rx) = create_channel(1);

            let task = async move {
                let stats_manager_handle = stats_manager.handle();
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle =
                    ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
                let mut receiver = pipeline.start(true, &mut runtime_handle)?;

                // Message router handles routing messages to per-input_id channels
                let mut message_router = MessageRouter::new(HashMap::new());

                let mut input_exhausted = false;

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
                        enqueue_msg = enqueue_input_rx.recv(), if !input_exhausted => {
                            match enqueue_msg {
                                Some(EnqueueInputMessage { input_id, plan_inputs_by_key, result_sender }) => {
                                    // Store the result sender in message router's output_senders
                                    message_router.insert_output_sender(input_id, result_sender);

                                    // Send inputs via local input_senders
                                    for (key, input_id, plan_input) in plan_inputs_by_key {
                                        if let Some(sender) = input_senders.get(&key) {
                                            // If send fails (e.g., channel closed), continue with other inputs
                                            let _ = sender.send(input_id, plan_input).await;
                                        }
                                    }
                                }
                                None => {
                                    input_senders.clear();
                                    input_exhausted = true;
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

            let handle = RuntimeTask::new(handle, task);
            Ok((handle, enqueue_input_tx))
        })?;

        // Now enqueue the inputs
        let enqueue_input_sender = if let Some(plan_state) = self.active_plans.plans.get_mut(&fingerprint) {
            // Add input_id to active_input_ids
            plan_state.active_input_ids.insert(input_id);
            plan_state.enqueue_input_sender.clone()
        } else {
            return Err(common_error::DaftError::ValueError(
                "Plan not found or already finished".to_string()
            ));
        };

        Ok(async move {
            // Create the channel for this input_id and get the receiver
            let (result_tx, result_rx) = create_channel(1);

            // Send enqueue_input message to the execution task
            let enqueue_msg = EnqueueInputMessage {
                input_id,
                plan_inputs_by_key,
                result_sender: result_tx,
            };

            // If send fails (e.g., channel closed due to cancellation), return None
            if enqueue_input_sender.send(enqueue_msg).await.is_err() {
                return Ok(None);
            }

            Ok(Some((input_id, result_rx)))
        }.boxed())
    }

    /// Enqueue inputs for a plan by fingerprint and return a future that sends the message
    pub fn enqueue_input(
        &mut self,
        fingerprint: u64,
        input_id: InputId,
        plan_inputs_by_key: Vec<(String, InputId, PlanInput)>,
    ) -> DaftResult<BoxFuture<'static, DaftResult<Option<(InputId, Receiver<Arc<MicroPartition>>)>>>> {
        let enqueue_input_sender = if let Some(plan_state) = self.active_plans.plans.get_mut(&fingerprint) {
            // Add input_id to active_input_ids
            plan_state.active_input_ids.insert(input_id);
            plan_state.enqueue_input_sender.clone()
        } else {
            return Err(common_error::DaftError::ValueError(
                "Plan not found or already finished".to_string()
            ));
        };

        Ok(async move {
            // Create the channel for this input_id and get the receiver
            let (result_tx, result_rx) = create_channel(1);

            // Send enqueue_input message to the execution task
            let enqueue_msg = EnqueueInputMessage {
                input_id,
                plan_inputs_by_key,
                result_sender: result_tx,
            };

            // If send fails (e.g., channel closed due to cancellation), return None
            if enqueue_input_sender.send(enqueue_msg).await.is_err() {
                return Ok(None);
            }

            Ok(Some((input_id, result_rx)))
        }.boxed())
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan).unwrap();
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
        let (physical_plan, _) = translate(&logical_plan).unwrap();
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
        let (physical_plan, _) = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();
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

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PyResultReceiver")
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
