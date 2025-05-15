use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_display::{mermaid::MermaidDisplayOptions, DisplayLevel};
use common_error::DaftResult;
use common_runtime::RuntimeTask;
use common_tracing::refresh_chrome_trace;
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    partitioning::{InMemoryPartitionSetCache, MicroPartitionSet, PartitionSetCache},
    MicroPartition, MicroPartitionRef,
};
use futures::{stream::BoxStream, Stream, StreamExt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    pyo3::{
        pyclass, pymethods, Bound, IntoPyObject, PyAny, PyObject, PyRef, PyRefMut, PyResult, Python,
    },
};

use crate::{
    channel::{create_channel, Receiver},
    get_or_init_runtime,
    pipeline::{physical_plan_to_pipeline, viz_pipeline_ascii, viz_pipeline_mermaid},
    progress_bar::{make_progress_bar_manager, ProgressBarManager},
    resource_manager::get_or_init_memory_manager,
    Error, ExecutionRuntimeContext,
};

#[cfg(feature = "python")]
#[pyclass]
struct LocalPartitionIterator {
    iter: Box<dyn Iterator<Item = DaftResult<PyObject>> + Send + Sync>,
}

#[cfg(feature = "python")]
#[pymethods]
impl LocalPartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        let iter = &mut slf.iter;
        Ok(py.allow_threads(|| iter.next().transpose())?)
    }
}

#[cfg(feature = "python")]
#[pyclass(frozen)]
struct LocalPartitionStream {
    stream: Arc<Mutex<BoxStream<'static, DaftResult<PyObject>>>>,
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
    pyclass(module = "daft.daft", name = "NativeExecutor")
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

    #[pyo3(signature = (local_physical_plan, psets, cfg, results_buffer_size=None))]
    pub fn run<'a>(
        &self,
        py: Python<'a>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        psets: HashMap<String, Vec<PyMicroPartition>>,
        cfg: PyDaftExecutionConfig,
        results_buffer_size: Option<usize>,
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
        let out = py.allow_threads(|| {
            self.executor
                .run(
                    &local_physical_plan.plan,
                    &psets,
                    cfg.config,
                    results_buffer_size,
                )
                .map(|res| res.into_iter())
        })?;
        let iter = Box::new(out.map(|part| {
            pyo3::Python::with_gil(|py| {
                Ok(PyMicroPartition::from(part?)
                    .into_pyobject(py)?
                    .unbind()
                    .into_any())
            })
        }));
        let part_iter = LocalPartitionIterator { iter };
        Ok(part_iter.into_pyobject(py)?.into_any())
    }

    #[pyo3(signature = (local_physical_plan, psets, cfg, results_buffer_size=None))]
    pub fn run_async<'a>(
        &self,
        py: Python<'a>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        psets: HashMap<String, Vec<PyMicroPartition>>,
        cfg: PyDaftExecutionConfig,
        results_buffer_size: Option<usize>,
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
        let res = py.allow_threads(|| {
            self.executor.run(
                &local_physical_plan.plan,
                &psets,
                cfg.config,
                results_buffer_size,
            )
        })?;
        let stream = Box::pin(res.into_stream().map(|part| {
            pyo3::Python::with_gil(|py| {
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

    pub fn repr_ascii(
        &self,
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        simple: bool,
    ) -> PyResult<String> {
        Ok(self
            .executor
            .repr_ascii(&logical_plan_builder.builder, cfg.config, simple))
    }

    pub fn repr_mermaid(
        &self,
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        options: MermaidDisplayOptions,
    ) -> PyResult<String> {
        Ok(self
            .executor
            .repr_mermaid(&logical_plan_builder.builder, cfg.config, options))
    }
}

#[derive(Debug, Clone)]
pub struct NativeExecutor {
    pb_manager: Option<Arc<dyn ProgressBarManager>>,
    enable_explain_analyze: bool,
}

impl Default for NativeExecutor {
    fn default() -> Self {
        Self {
            pb_manager: should_enable_progress_bar().then(make_progress_bar_manager),
            enable_explain_analyze: should_enable_explain_analyze(),
        }
    }
}

impl NativeExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_progress_bar_manager(mut self, pb_manager: Arc<dyn ProgressBarManager>) -> Self {
        self.pb_manager = Some(pb_manager);
        self
    }

    pub fn enable_explain_analyze(mut self, b: bool) -> Self {
        self.enable_explain_analyze = b;
        self
    }

    pub fn run(
        &self,
        local_physical_plan: &LocalPhysicalPlanRef,
        psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
        cfg: Arc<DaftExecutionConfig>,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<ExecutionEngineResult> {
        refresh_chrome_trace();
        let pipeline = physical_plan_to_pipeline(local_physical_plan, psets, &cfg)?;
        let (tx, rx) = create_channel(results_buffer_size.unwrap_or(0));

        let pb_manager = self.pb_manager.clone();
        let enable_explain_analyze = self.enable_explain_analyze;
        // todo: split this into a run and run_async method
        // the run_async should spawn a task instead of a thread like this
        let handle = get_or_init_runtime().spawn(async move {
            let memory_manager = get_or_init_memory_manager();
            let mut runtime_handle = ExecutionRuntimeContext::new(
                cfg.default_morsel_size,
                memory_manager.clone(),
                pb_manager,
            );
            let receiver = pipeline.start(true, &mut runtime_handle)?;

            while let Some(val) = receiver.recv().await {
                if tx.send(val).await.is_err() {
                    break;
                }
            }

            while let Some(result) = runtime_handle.join_next().await {
                match result {
                    Ok(Err(e)) => {
                        runtime_handle.shutdown().await;
                        return DaftResult::Err(e.into());
                    }
                    Err(e) => {
                        runtime_handle.shutdown().await;
                        return DaftResult::Err(Error::JoinError { source: e }.into());
                    }
                    _ => {}
                }
            }
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
            Ok(())
        });

        Ok(ExecutionEngineResult {
            handle,
            receiver: rx,
        })
    }

    fn repr_ascii(
        &self,
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let physical_plan = translate(&logical_plan).unwrap();
        let pipeline_node =
            physical_plan_to_pipeline(&physical_plan, &InMemoryPartitionSetCache::empty(), &cfg)
                .unwrap();

        viz_pipeline_ascii(pipeline_node.as_ref(), simple)
    }

    fn repr_mermaid(
        &self,
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        options: MermaidDisplayOptions,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let physical_plan = translate(&logical_plan).unwrap();
        let pipeline_node =
            physical_plan_to_pipeline(&physical_plan, &InMemoryPartitionSetCache::empty(), &cfg)
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

fn should_enable_progress_bar() -> bool {
    let progress_var_name = "DAFT_PROGRESS_BAR";
    if let Ok(val) = std::env::var(progress_var_name) {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        true // Return true when env var is not set
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
                    let join_result = get_or_init_runtime()
                        .block_on_current_thread(handle)
                        .expect("Execution engine thread panicked");
                    match join_result {
                        Ok(()) => None,
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
                        let join_result = handle.await;
                        match join_result {
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
