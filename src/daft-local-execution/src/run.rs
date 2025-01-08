use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_tracing::refresh_chrome_trace;
use daft_local_plan::{translate, LocalPhysicalPlan};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    partitioning::{InMemoryPartitionSetCache, MicroPartitionSet, PartitionSetCache},
    MicroPartition, MicroPartitionRef,
};
use futures::Stream;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    pyo3::{pyclass, pymethods, IntoPy, PyObject, PyRef, PyRefMut, PyResult, Python},
};

use crate::{
    channel::{create_channel, Receiver},
    pipeline::{physical_plan_to_pipeline, viz_pipeline},
    progress_bar::make_progress_bar_manager,
    resource_manager::get_or_init_memory_manager,
    Error, ExecutionRuntimeContext,
};

#[cfg(feature = "python")]
#[pyclass]
struct LocalPartitionIterator {
    iter: Box<dyn Iterator<Item = DaftResult<PyObject>> + Send>,
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

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "NativeExecutor")
)]
pub struct PyNativeExecutor {
    executor: NativeExecutor,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyNativeExecutor {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(Self {
                executor: NativeExecutor::from_logical_plan_builder(&logical_plan_builder.builder)?,
            })
        })
    }

    pub fn run(
        &self,
        py: Python,
        psets: HashMap<String, Vec<PyMicroPartition>>,
        cfg: PyDaftExecutionConfig,
        results_buffer_size: Option<usize>,
    ) -> PyResult<PyObject> {
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
                .run(&psets, cfg.config, results_buffer_size)
                .map(|res| res.into_iter())
        })?;
        let iter = Box::new(out.map(|part| {
            part.map(|p| pyo3::Python::with_gil(|py| PyMicroPartition::from(p).into_py(py)))
        }));
        let part_iter = LocalPartitionIterator { iter };
        Ok(part_iter.into_py(py))
    }
}

pub struct NativeExecutor {
    local_physical_plan: Arc<LocalPhysicalPlan>,
    cancel: CancellationToken,
}

impl NativeExecutor {
    pub fn from_logical_plan_builder(
        logical_plan_builder: &LogicalPlanBuilder,
    ) -> DaftResult<Self> {
        let logical_plan = logical_plan_builder.build();
        let local_physical_plan = translate(&logical_plan)?;

        Ok(Self {
            local_physical_plan,
            cancel: CancellationToken::new(),
        })
    }

    pub fn run(
        &self,
        psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
        cfg: Arc<DaftExecutionConfig>,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<ExecutionEngineResult> {
        run_local(
            &self.local_physical_plan,
            psets,
            cfg,
            results_buffer_size,
            self.cancel.clone(),
        )
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

pub fn run_local(
    physical_plan: &LocalPhysicalPlan,
    psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
    cfg: Arc<DaftExecutionConfig>,
    results_buffer_size: Option<usize>,
    cancel: CancellationToken,
) -> DaftResult<ExecutionEngineResult> {
    refresh_chrome_trace();
    let pipeline = physical_plan_to_pipeline(physical_plan, psets, &cfg)?;
    let (tx, rx) = create_channel(results_buffer_size.unwrap_or(0));
    let handle = std::thread::spawn(move || {
        let pb_manager = should_enable_progress_bar().then(make_progress_bar_manager);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");
        let execution_task = async {
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
            if should_enable_explain_analyze() {
                let curr_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis();
                let file_name = format!("explain-analyze-{curr_ms}-mermaid.md");
                let mut file = File::create(file_name)?;
                writeln!(file, "```mermaid\n{}\n```", viz_pipeline(pipeline.as_ref()))?;
            }
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
