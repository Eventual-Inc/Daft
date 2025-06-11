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
use common_tracing::{flush_opentelemetry_providers, refresh_chrome_trace};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    partitioning::{InMemoryPartitionSetCache, MicroPartitionSet, PartitionSetCache},
    MicroPartition, MicroPartitionRef,
};
use daft_scan::{python::pylib::PyScanTask, ScanTaskRef};
use futures::{stream::BoxStream, Stream, StreamExt};
use tokio::sync::{oneshot, Mutex};
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
    channel::{create_channel, Receiver, Sender},
    pipeline::{
        get_pipeline_relationship_mapping, physical_plan_to_pipeline, viz_pipeline_ascii,
        viz_pipeline_mermaid, RelationshipInformation, TranslationContext,
    },
    progress_bar::{make_progress_bar_manager, ProgressBarManager},
    resource_manager::get_or_init_memory_manager,
    ExecutionRuntimeContext,
};

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "InProgressSwordfishPlan")]
pub(crate) struct PyInProgressSwordfishPlan {
    result_cache: ResultCache,
    channel_scan_senders: ChannelScanSenders,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyInProgressSwordfishPlan {
    fn get_result<'a>(&self, py: Python<'a>, key: usize) -> PyResult<Bound<'a, PyAny>> {
        let cache = self.result_cache.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = cache.get(key).await;
            pyo3::Python::with_gil(|py| {
                Ok(PyMicroPartition::from(result)
                    .into_pyobject(py)?
                    .unbind()
                    .into_any())
            })
        })
    }

    fn put_input<'a>(&self, py: Python<'a>, morsel_id: usize, input: PyScanTask) -> PyResult<Bound<'a, PyAny>> {
        let senders = self.channel_scan_senders.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // for now, we only support one channel
            senders.enqueue_input(morsel_id, input.0.clone(), 0).await?;
            Ok(())
        })
    }
}

/// A cache that stores micropartitions by morsel ID and allows async waiting for results
#[derive(Debug, Clone)]
struct ResultCache {
    cache: Arc<Mutex<HashMap<usize, Arc<MicroPartition>>>>,
    pending_gets: Arc<Mutex<HashMap<usize, Vec<oneshot::Sender<Arc<MicroPartition>>>>>>,
}

impl ResultCache {
    fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            pending_gets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Put a result into the cache and notify any waiting getters
    async fn put(&self, key: usize, value: Arc<MicroPartition>) {
        let mut cache = self.cache.lock().await;
        cache.insert(key, value.clone());

        let mut pending = self.pending_gets.lock().await;
        if let Some(senders) = pending.remove(&key) {
            for sender in senders {
                let _ = sender.send(value.clone());
            }
        }
    }

    /// Get a result from the cache, waiting if it doesn't exist yet
    async fn get(&self, key: usize) -> Arc<MicroPartition> {
        // First check if the value is already in the cache
        {
            let cache = self.cache.lock().await;
            if let Some(value) = cache.get(&key) {
                return value.clone();
            }
        }

        // If not, set up a oneshot channel to wait for the value
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_gets.lock().await;
            pending.entry(key).or_insert_with(Vec::new).push(tx);
        }

        rx.await.expect("Sender dropped")
    }
}

/// A collection of channel scan senders for queueing scan tasks
#[derive(Clone)]
struct ChannelScanSenders {
    channel_scan_senders: Vec<Sender<(usize, ScanTaskRef)>>,
}

impl ChannelScanSenders {
    fn new(channel_scan_senders: Vec<Sender<(usize, ScanTaskRef)>>) -> Self {
        assert_eq!(
            channel_scan_senders.len(),
            1,
            "Only one channel is supported for now"
        );
        Self {
            channel_scan_senders,
        }
    }

    /// Enqueue a scan task to the specified channel index
    async fn enqueue_input(&self, morsel_id: usize, scan_task: ScanTaskRef, channel_idx: usize) -> DaftResult<()> {
        assert_eq!(channel_idx, 0, "Only one channel is supported for now");

        self.channel_scan_senders[channel_idx]
            .send((morsel_id, scan_task))
            .await
            .map_err(|_| {
                common_error::DaftError::External("Failed to send scan task to channel".into())
            })?;

        Ok(())
    }
}

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

    pub fn get_relationship_info(
        &self,
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<RelationshipInformation> {
        Ok(self
            .executor
            .get_relationship_info(&logical_plan_builder.builder, cfg.config))
    }

    #[pyo3(signature = (local_physical_plan, cfg))]
    fn run_distributed(
        &self,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<PyInProgressSwordfishPlan> {
        let (cache, senders) = self
            .executor
            .run_into_cache(&local_physical_plan.plan, cfg.config)?;
        Ok(PyInProgressSwordfishPlan {
            result_cache: cache,
            channel_scan_senders: senders,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NativeExecutor {
    cancel: CancellationToken,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    pb_manager: Option<Arc<dyn ProgressBarManager>>,
    enable_explain_analyze: bool,
}

impl Default for NativeExecutor {
    fn default() -> Self {
        Self {
            cancel: CancellationToken::new(),
            runtime: None,
            pb_manager: should_enable_progress_bar().then(make_progress_bar_manager),
            enable_explain_analyze: should_enable_explain_analyze(),
        }
    }
}

impl NativeExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_runtime(mut self, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
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
        let cancel = self.cancel.clone();
        let ctx = TranslationContext::new();
        let mut channel_scan_senders = Vec::new();
        let pipeline = physical_plan_to_pipeline(
            local_physical_plan,
            psets,
            &cfg,
            &ctx,
            &mut channel_scan_senders,
        )?;
        let (tx, rx) = create_channel(results_buffer_size.unwrap_or(0));

        let rt = self.runtime.clone();
        let pb_manager = self.pb_manager.clone();
        let enable_explain_analyze = self.enable_explain_analyze;
        // todo: split this into a run and run_async method
        // the run_async should spawn a task instead of a thread like this
        let handle = std::thread::spawn(move || {
            let runtime = rt.unwrap_or_else(|| {
                Arc::new(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create tokio runtime"),
                )
            });
            let execution_task = async {
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle = ExecutionRuntimeContext::new(
                    cfg.default_morsel_size,
                    memory_manager.clone(),
                    pb_manager,
                );
                let receiver = pipeline.start(true, &mut runtime_handle)?;

                while let Some((_morsel_id, inner_rx)) = receiver.recv().await {
                    while let Some(morsel) = inner_rx.recv().await {
                        if tx.send(morsel).await.is_err() {
                            break;
                        }
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
                Ok(())
            };

            let local_set = tokio::task::LocalSet::new();
            local_set.block_on(&runtime, async {
                tokio::select! {
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
                }
            })
        });

        Ok(ExecutionEngineResult {
            handle,
            receiver: rx,
        })
    }

    /// Similar to run() but returns a ResultCache and ChannelScanSenders separately
    fn run_into_cache(
        &self,
        local_physical_plan: &LocalPhysicalPlanRef,
        cfg: Arc<DaftExecutionConfig>,
    ) -> DaftResult<(ResultCache, ChannelScanSenders)> {
        refresh_chrome_trace();
        let cancel = self.cancel.clone();
        let ctx = TranslationContext::new();
        let mut channel_scan_senders = Vec::new();
        let pipeline = physical_plan_to_pipeline(
            local_physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
            &mut channel_scan_senders,
        )?;
        let result_cache = ResultCache::new();

        let rt = self.runtime.clone();
        let pb_manager = self.pb_manager.clone();
        let enable_explain_analyze = self.enable_explain_analyze;
        let cache_clone = result_cache.clone();

        // Spawn a task to run the pipeline and populate the cache
        std::thread::spawn(move || {
            let runtime = rt.unwrap_or_else(|| {
                Arc::new(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create tokio runtime"),
                )
            });
            let execution_task = async move {
                let memory_manager = get_or_init_memory_manager();
                let mut runtime_handle = ExecutionRuntimeContext::new(
                    cfg.default_morsel_size,
                    memory_manager.clone(),
                    pb_manager,
                );
                let receiver = pipeline.start(true, &mut runtime_handle)?;

                // Consume the receiver and store results in the cache by morsel_id
                while let Some((morsel_id, inner_rx)) = receiver.recv().await {
                    let results = inner_rx.into_stream().collect::<Vec<_>>().await;
                    let concated = MicroPartition::concat(results)?;
                    cache_clone.put(morsel_id, concated.into()).await;
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
                Ok(())
            };

            let local_set = tokio::task::LocalSet::new();
            let _result = local_set.block_on(&runtime, async {
                tokio::select! {
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
                }
            });
        });

        Ok((result_cache, ChannelScanSenders::new(channel_scan_senders)))
    }

    fn repr_ascii(
        &self,
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let physical_plan = translate(&logical_plan).unwrap();
        let ctx = TranslationContext::new();
        let mut dummy_senders = Vec::new();
        let pipeline_node = physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
            &mut dummy_senders,
        )
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
        let ctx = TranslationContext::new();
        let pipeline_node = physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
            &mut Vec::new(),
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
        &self,
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
    ) -> RelationshipInformation {
        let logical_plan = logical_plan_builder.build();
        let physical_plan = translate(&logical_plan).unwrap();
        let ctx = TranslationContext::new();
        let pipeline_node = physical_plan_to_pipeline(
            &physical_plan,
            &InMemoryPartitionSetCache::empty(),
            &cfg,
            &ctx,
            &mut Vec::new(),
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
    handle: Option<std::thread::JoinHandle<DaftResult<()>>>,
}

impl Iterator for ExecutionEngineReceiverIterator {
    type Item = DaftResult<Arc<MicroPartition>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv().ok() {
            Some(part) => Some(Ok(part)),
            None => {
                if self.handle.is_some() {
                    let join_result = self
                        .handle
                        .take()
                        .unwrap()
                        .join()
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
    handle: std::thread::JoinHandle<DaftResult<()>>,
    receiver: Receiver<Arc<MicroPartition>>,
}

impl ExecutionEngineResult {
    pub fn into_stream(self) -> impl Stream<Item = DaftResult<Arc<MicroPartition>>> {
        struct StreamState {
            receiver: Receiver<Arc<MicroPartition>>,
            handle: Option<std::thread::JoinHandle<DaftResult<()>>>,
        }

        let state = StreamState {
            receiver: self.receiver,
            handle: Some(self.handle),
        };

        futures::stream::unfold(state, |mut state| async {
            match state.receiver.recv().await {
                Some(part) => Some((Ok(part), state)),
                None => {
                    if state.handle.is_some() {
                        let join_result = state
                            .handle
                            .take()
                            .unwrap()
                            .join()
                            .expect("Execution engine thread panicked");
                        match join_result {
                            Ok(()) => None,
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
