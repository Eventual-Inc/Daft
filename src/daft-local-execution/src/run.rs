use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_tracing::refresh_chrome_trace;
use daft_local_plan::{translate, LocalPhysicalPlan};
use daft_micropartition::MicroPartition;
use futures::{Stream, StreamExt};
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
    Error, ExecutionRuntimeHandle,
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

#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct NativeExecutor {
    local_physical_plan: Arc<LocalPhysicalPlan>,
}

/// A blocking iterator adapter for any Stream.
pub struct BlockingStreamIter<S> {
    stream: Pin<Box<S>>,
}

impl<S> BlockingStreamIter<S> {
    /// Creates a new BlockingStreamIter from a Stream.
    pub fn new(stream: S) -> Self
    where
        S: Stream + 'static,
    {
        Self {
            stream: Box::pin(stream),
        }
    }
}

impl<S> Iterator for BlockingStreamIter<S>
where
    S: Stream + Unpin + 'static,
{
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        futures::executor::block_on(self.stream.as_mut().next())
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl NativeExecutor {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let logical_plan = logical_plan_builder.builder.build();
            let local_physical_plan = translate(&logical_plan)?;
            Ok(Self {
                local_physical_plan,
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
        let native_psets: HashMap<String, Vec<Arc<MicroPartition>>> = psets
            .into_iter()
            .map(|(part_id, parts)| {
                (
                    part_id,
                    parts
                        .into_iter()
                        .map(std::convert::Into::into)
                        .collect::<Vec<Arc<MicroPartition>>>(),
                )
            })
            .collect();

        let out = py.allow_threads(|| {
            run_local(
                &self.local_physical_plan,
                native_psets,
                cfg.config,
                results_buffer_size,
            )
        })?;

        let out = BlockingStreamIter::new(out);

        let iter = Box::new(out.map(|part| {
            part.map(|p| pyo3::Python::with_gil(|py| PyMicroPartition::from(p).into_py(py)))
        }));
        let part_iter = LocalPartitionIterator { iter };
        Ok(part_iter.into_py(py))
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

pub type PartitionResult = DaftResult<Arc<MicroPartition>>;
pub type SendableStream<T> = dyn Stream<Item = T> + Send;

/// A pinned boxed stream that can be sent across thread boundaries
pub type PinnedStream<T> = Pin<Box<SendableStream<T>>>;

pub fn run_local(
    physical_plan: &LocalPhysicalPlan,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
    cfg: Arc<DaftExecutionConfig>,
    results_buffer_size: Option<usize>,
) -> DaftResult<PinnedStream<PartitionResult>> {
    refresh_chrome_trace();
    let mut pipeline = physical_plan_to_pipeline(physical_plan, &psets, &cfg)?;
    let (tx, rx) = create_channel(results_buffer_size.unwrap_or(1));
    let handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");
        let execution_task = async {
            let mut runtime_handle = ExecutionRuntimeHandle::new(cfg.default_morsel_size);
            let mut receiver = pipeline.start(true, &mut runtime_handle)?.get_receiver();

            while let Some(val) = receiver.recv().await {
                if tx.send(val.as_data().clone()).await.is_err() {
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
                _ = tokio::signal::ctrl_c() => {
                    log::info!("Received Ctrl-C, shutting down execution engine");
                    Ok(())
                }
                result = execution_task => result,
            }
        })
    });

    struct ReceiverStream {
        receiver: Receiver<Arc<MicroPartition>>,
        handle: Option<std::thread::JoinHandle<DaftResult<()>>>,
    }

    impl Stream for ReceiverStream {
        type Item = DaftResult<Arc<MicroPartition>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(part)) => Poll::Ready(Some(Ok(part))),
                Poll::Ready(None) => {
                    if self.handle.is_some() {
                        let join_result = self
                            .handle
                            .take()
                            .unwrap()
                            .join()
                            .expect("Execution engine thread panicked");
                        match join_result {
                            Ok(()) => Poll::Ready(None),
                            Err(e) => Poll::Ready(Some(Err(e))),
                        }
                    } else {
                        Poll::Ready(None)
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
    Ok(Box::pin(ReceiverStream {
        receiver: rx,
        handle: Some(handle),
    }))
}
