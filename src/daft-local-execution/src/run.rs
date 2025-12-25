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
use futures::Stream;
use tokio::{runtime::Handle, sync::Mutex};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    pyo3::{Bound, IntoPyObject, PyAny, PyRef, PyResult, Python, pyclass, pymethods},
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{
        RelationshipInformation, RuntimeContext, get_pipeline_relationship_mapping,
        translate_physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid,
    },
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

    #[pyo3(signature = (local_physical_plan, psets, daft_ctx, results_buffer_size=None, context=None))]
    pub fn run<'a>(
        &self,
        py: Python<'a>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        psets: HashMap<String, Vec<PyMicroPartition>>,
        daft_ctx: &PyDaftContext,
        results_buffer_size: Option<usize>,
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
                results_buffer_size,
                context,
            )
        })?;

        let py_execution_result = PyExecutionEngineResult {
            result: Arc::new(Mutex::new(Some(res))),
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
pub struct NativeExecutor {
    cancel: CancellationToken,
    enable_explain_analyze: bool,
}

impl Default for NativeExecutor {
    fn default() -> Self {
        Self {
            cancel: CancellationToken::new(),
            enable_explain_analyze: should_enable_explain_analyze(),
        }
    }
}

impl NativeExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable_explain_analyze(mut self, b: bool) -> Self {
        self.enable_explain_analyze = b;
        self
    }

    pub fn run(
        &self,
        local_physical_plan: &LocalPhysicalPlanRef,
        psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        results_buffer_size: Option<usize>,
        additional_context: Option<HashMap<String, String>>,
    ) -> DaftResult<ExecutionEngineResult> {
        let cancel = self.cancel.clone();
        let additional_context = additional_context.unwrap_or_default();
        let ctx = RuntimeContext::new_with_context(additional_context.clone());
        let pipeline =
            translate_physical_plan_to_pipeline(local_physical_plan, psets, &exec_cfg, &ctx)?;

        let (tx, rx) = create_channel(results_buffer_size.unwrap_or(0));
        let enable_explain_analyze = self.enable_explain_analyze;

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
        let task = async move {
            let stats_manager_handle = stats_manager.handle();
            let execution_task = async {
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle =
                    ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
                let receiver = pipeline.start(exec_cfg.maintain_order, &mut runtime_handle)?;

                while let Some(val) = receiver.recv().await {
                    if tx.send(val).await.is_err() {
                        return Ok(());
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

        Ok(ExecutionEngineResult {
            receiver: rx,
            handle: RuntimeTask::new(handle, task),
        })
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let physical_plan = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let pipeline_node = translate_physical_plan_to_pipeline(
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
        let physical_plan = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let pipeline_node = translate_physical_plan_to_pipeline(
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
        let physical_plan = translate(&logical_plan).unwrap();
        let ctx = RuntimeContext::new();
        let pipeline_node = translate_physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
        )
        .unwrap();
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

type ExecutionEngineFinalResult = DaftResult<Vec<(usize, StatSnapshot)>>;

pub struct ExecutionEngineResult {
    handle: RuntimeTask<ExecutionEngineFinalResult>,
    receiver: Receiver<Arc<MicroPartition>>,
}

impl ExecutionEngineResult {
    async fn next(&self) -> Option<Arc<MicroPartition>> {
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
                Some(part) => Some((Ok(part), state)),
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
            let result = result.lock().await;
            let part = result
                .as_ref()
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
