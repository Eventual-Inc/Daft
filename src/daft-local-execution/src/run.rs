use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::DaftResult;
use common_metrics::{NodeID, StatSnapshot};
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{DaftContext, Subscriber};
use daft_local_plan::{InputId, LocalPhysicalPlanRef, ResolvedInput, SourceId, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use futures::{FutureExt, Stream, future::BoxFuture};
use tokio::{runtime::Handle, sync::Mutex};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    pyo3::{Bound, PyAny, PyRef, PyResult, Python, pyclass, pymethods},
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{
        RelationshipInformation, RuntimeContext, get_pipeline_relationship_mapping,
        translate_physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid,
    },
    pipeline_message::PipelineMessage,
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{QueryEndState, RuntimeStatsManager},
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

pub(crate) struct EnqueueInputMessage {
    /// The input_id for this enqueue operation
    input_id: InputId,
    /// Plan inputs grouped by source_id
    inputs: HashMap<SourceId, ResolvedInput>,
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

    #[pyo3(signature = (local_physical_plan, daft_ctx, input_id, inputs, context=None))]
    pub fn run<'py>(
        &self,
        py: Python<'py>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        daft_ctx: &PyDaftContext,
        input_id: InputId,
        inputs: daft_local_plan::python::PyResolvedInputs,
        context: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let daft_ctx: &DaftContext = daft_ctx.into();
        let plan = local_physical_plan.plan.clone();
        let exec_cfg = daft_ctx.execution_config();
        let subscribers = daft_ctx.subscribers();
        let enqueue_future = {
            self.executor.run(
                &plan,
                exec_cfg,
                subscribers,
                context,
                input_id,
                inputs.inner,
            )?
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = enqueue_future.await;
            Ok(PyExecutionEngineResult {
                result: Arc::new(Mutex::new(Some(result))),
            })
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
}

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
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        input_id: InputId,
        inputs: HashMap<SourceId, ResolvedInput>,
    ) -> DaftResult<BoxFuture<'static, ExecutionEngineResult>> {
        let cancel = self.cancel.clone();
        let additional_context = additional_context.unwrap_or_default();
        let ctx = RuntimeContext::new_with_context(additional_context.clone());
        let (mut pipeline, input_senders) =
            translate_physical_plan_to_pipeline(local_physical_plan, &exec_cfg, &ctx)?;

        let (tx, rx) = create_channel::<Arc<MicroPartition>>(1);
        let enable_explain_analyze = should_enable_explain_analyze();

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

        let (enqueue_input_tx, mut enqueue_input_rx) = create_channel::<EnqueueInputMessage>(1);

        let task = async move {
            let stats_manager_handle = stats_manager.handle();
            let execution_task = async {
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle =
                    ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
                let mut receiver = pipeline.start(true, &mut runtime_handle)?;

                if let Some(message) = enqueue_input_rx.recv().await {
                    for (key, plan_input) in message.inputs {
                        if let Some(sender) = input_senders.get(&key) {
                            let _ = sender.send(message.input_id, plan_input).await;
                        }
                    }
                }
                drop(input_senders);

                while let Some(msg) = receiver.recv().await {
                    // Extract partition from Morsel messages, ignore Flush
                    if let PipelineMessage::Morsel { partition, .. } = msg {
                        if tx.send(partition).await.is_err() {
                            return Ok(());
                        }
                    }
                }

                runtime_handle.shutdown().await
            };

            let (result, finish_status) = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    log::info!("Execution engine cancelled");
                (Ok(()), QueryEndState::Cancelled)
                }
                _ = tokio::signal::ctrl_c() => {
                    log::info!("Received Ctrl-C, shutting down execution engine");
                (Ok(()), QueryEndState::Cancelled)
                }
                result = execution_task => {
                    let status = if result.is_err() {
                        QueryEndState::Failed
                    } else {
                        QueryEndState::Finished
                    };
                    (result, status)
                },
            };

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

        Ok(async move {
            let enqueue_msg = EnqueueInputMessage { input_id, inputs };

            let _ = enqueue_input_tx.send(enqueue_msg).await;
            ExecutionEngineResult {
                handle,
                receiver: rx,
            }
        }
        .boxed())
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

impl Drop for NativeExecutor {
    fn drop(&mut self) {
        self.cancel.cancel();
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

type ExecutionEngineFinalResult = DaftResult<Vec<(NodeID, StatSnapshot)>>;

pub struct ExecutionEngineResult {
    handle: RuntimeTask<ExecutionEngineFinalResult>,
    receiver: Receiver<Arc<MicroPartition>>,
}

impl ExecutionEngineResult {
    async fn next(&mut self) -> Option<Arc<MicroPartition>> {
        self.receiver.recv().await
    }

    async fn finish(self) -> ExecutionEngineFinalResult {
        drop(self.receiver);
        let result = self.handle.await;
        match result {
            Ok(Ok(final_stats)) => Ok(final_stats),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    }

    // Should be used independently of next() and finish()
    pub fn into_stream(self) -> impl Stream<Item = DaftResult<Arc<MicroPartition>>> {
        struct StreamState {
            receiver: Receiver<Arc<MicroPartition>>,
            handle: Option<RuntimeTask<ExecutionEngineFinalResult>>,
        }

        let state = StreamState {
            receiver: self.receiver,
            handle: Some(self.handle),
        };

        futures::stream::unfold(state, |mut state| async {
            match state.receiver.recv().await {
                Some(partition) => Some((Ok(partition), state)),
                None => {
                    if let Some(handle) = state.handle.take() {
                        let result = handle.await;
                        match result {
                            Ok(Ok(_final_stats)) => None,
                            Ok(Err(e)) => Some((Err(e), state)),
                            Err(e) => Some((Err(e), state)),
                        }
                    } else {
                        None
                    }
                }
            }
        })
    }
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PyExecutionEngineResult", frozen)
)]
pub struct PyExecutionEngineResult {
    result: Arc<Mutex<Option<ExecutionEngineResult>>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyExecutionEngineResult {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let result = self.result.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut result = result.lock().await;
            let part = result
                .as_mut()
                .expect("ExecutionEngineResult.__anext__() should not be called after finish().")
                .next()
                .await;
            Ok(part.map(PyMicroPartition::from))
        })
    }

    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = self.result.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut result = result.lock().await;
            let stats = result
                .take()
                .expect("ExecutionEngineResult.finish() should not be called more than once.")
                .finish()
                .await?;
            Ok(bincode::encode_to_vec(&stats, bincode::config::legacy())
                .expect("Failed to serialize stats object"))
        })
    }
}
