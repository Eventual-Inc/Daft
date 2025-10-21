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
use common_runtime::RuntimeTask;
use common_tracing::flush_opentelemetry_providers;
use daft_context::{DaftContext, Subscriber};
use daft_local_plan::{LocalPhysicalPlanRef, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    MicroPartition, MicroPartitionRef,
    partitioning::{InMemoryPartitionSetCache, MicroPartitionSet, PartitionSetCache},
};
use futures::{Stream, StreamExt, stream::BoxStream};
use tokio::{runtime::Handle, sync::Mutex};
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    pyo3::{Bound, IntoPyObject, Py, PyAny, PyRef, PyRefMut, PyResult, Python, pyclass, pymethods},
};

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{
        RelationshipInformation, RuntimeContext, get_pipeline_relationship_mapping,
        translate_physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid,
    },
    resource_manager::get_or_init_memory_manager,
    runtime_stats::RuntimeStatsManager,
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

#[cfg(feature = "python")]
#[pyclass]
struct LocalPartitionIterator {
    iter: Box<dyn Iterator<Item = DaftResult<Py<PyAny>>> + Send + Sync>,
}

#[cfg(feature = "python")]
#[pymethods]
impl LocalPartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let iter = &mut slf.iter;
        Ok(py.detach(|| iter.next().transpose())?)
    }
}

#[cfg(feature = "python")]
#[pyclass(frozen)]
struct LocalPartitionStream {
    stream: Arc<Mutex<BoxStream<'static, DaftResult<Py<PyAny>>>>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl LocalPartitionStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = stream.lock().await;
            let part = stream.next().await;
            Ok(part.transpose()?)
        })
    }
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
        let out = py.detach(|| {
            self.executor
                .run(
                    &local_physical_plan.plan,
                    &psets,
                    daft_ctx.execution_config(),
                    daft_ctx.subscribers(),
                    results_buffer_size,
                    context,
                )
                .map(|res| res.into_iter())
        })?;
        let iter = Box::new(out.map(|part| {
            pyo3::Python::attach(|py| {
                Ok(PyMicroPartition::from(part?)
                    .into_pyobject(py)?
                    .unbind()
                    .into_any())
            })
        }));
        let part_iter = LocalPartitionIterator { iter };
        Ok(part_iter.into_pyobject(py)?.into_any())
    }

    #[pyo3(signature = (local_physical_plan, psets, exec_cfg, results_buffer_size=None, context=None))]
    pub fn run_async<'a>(
        &self,
        py: Python<'a>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        psets: HashMap<String, Vec<PyMicroPartition>>,
        exec_cfg: PyDaftExecutionConfig,
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
        let res = py.detach(|| {
            self.executor.run(
                &local_physical_plan.plan,
                &psets,
                exec_cfg.config,
                vec![],
                results_buffer_size,
                context,
            )
        })?;
        let stream = Box::pin(res.into_stream().map(|part| {
            pyo3::Python::attach(|py| {
                Ok(PyMicroPartition::from(part?)
                    .into_pyobject(py)?
                    .unbind()
                    .into_any())
            })
        }));
        let stream = LocalPartitionStream {
            stream: Arc::new(Mutex::new(stream)),
        };
        Ok(stream.into_pyobject(py)?.into_any())
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
        let ctx = RuntimeContext::new_with_context(additional_context.unwrap_or_default());
        let pipeline =
            translate_physical_plan_to_pipeline(local_physical_plan, psets, &exec_cfg, &ctx)?;

        let (tx, rx) = create_channel(results_buffer_size.unwrap_or(0));

        let handle = get_global_runtime();
        let enable_explain_analyze = self.enable_explain_analyze;

        // Spawn execution on the global runtime - returns immediately
        let stats_manager = RuntimeStatsManager::try_new(handle, &pipeline, subscribers)?;
        let task = async move {
            let stats_manager_handle = stats_manager.handle();
            let execution_task = async {
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle =
                    ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
                let receiver = pipeline.start(true, &mut runtime_handle)?;

                while let Some(val) = receiver.recv().await {
                    if tx.send(val).await.is_err() {
                        break;
                    }
                }

                while let Some(result) = runtime_handle.join_next().await {
                    match result {
                        Ok(Err(e)) | Err(e) => {
                            runtime_handle.shutdown().await;
                            return DaftResult::Err(e.into());
                        }
                        _ => {}
                    }
                }
                Ok(())
            };

            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    log::info!("Execution engine cancelled");
                    Ok(())
                }
                _ = tokio::signal::ctrl_c() => {
                    log::info!("Received Ctrl-C, shutting down execution engine");
                    Ok(())
                }
                result = execution_task => result,
            };

            // Finish the stats manager
            stats_manager.finish().await;

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
            result
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

pub struct ExecutionEngineReceiverIterator {
    receiver: kanal::Receiver<Arc<MicroPartition>>,
    handle: Option<RuntimeTask<DaftResult<()>>>,
}

impl Iterator for ExecutionEngineReceiverIterator {
    type Item = DaftResult<Arc<MicroPartition>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv().ok() {
            Some(part) => Some(Ok(part)),
            None => {
                if let Some(handle) = self.handle.take() {
                    let runtime = get_global_runtime();
                    let join_result = runtime.block_on(handle);
                    match join_result {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(Err(e)),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            }
        }
    }
}

pub struct ExecutionEngineResult {
    handle: RuntimeTask<DaftResult<()>>,
    receiver: Receiver<Arc<MicroPartition>>,
}

impl ExecutionEngineResult {
    pub fn into_stream(self) -> impl Stream<Item = DaftResult<Arc<MicroPartition>>> {
        struct StreamState {
            receiver: Receiver<Arc<MicroPartition>>,
            handle: Option<RuntimeTask<DaftResult<()>>>,
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
                            Ok(Ok(())) => None,
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

impl IntoIterator for ExecutionEngineResult {
    type Item = DaftResult<Arc<MicroPartition>>;
    type IntoIter = ExecutionEngineReceiverIterator;

    fn into_iter(self) -> Self::IntoIter {
        ExecutionEngineReceiverIterator {
            receiver: self.receiver.into_inner().to_sync(),
            handle: Some(self.handle),
        }
    }
}
