use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use common_error::{DaftError, DaftResult};
use common_tracing::refresh_chrome_trace;
use daft_core::schema::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{translate, LocalPhysicalPlan};

#[cfg(feature = "python")]
use {
    daft_micropartition::python::PyMicroPartition,
    daft_plan::PyLogicalPlanBuilder,
    pyo3::{pyclass, pymethods, IntoPy, PyObject, PyRef, PyRefMut, PyResult, Python},
};

use crate::{
    channel::{create_channel, create_single_channel, SingleReceiver},
    pipeline::physical_plan_to_pipeline,
    Error, ExecutionRuntimeHandle, NUM_CPUS,
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
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let iter = &mut slf.iter;
        Ok(py.allow_threads(|| iter.next().transpose())?)
    }
}

#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct NativeExecutor {
    local_physical_plan: Arc<LocalPhysicalPlan>,
}

#[cfg(feature = "python")]
#[pymethods]
impl NativeExecutor {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python<'_>,
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
    ) -> PyResult<PyObject> {
        let native_psets: HashMap<String, Vec<Arc<MicroPartition>>> = psets
            .into_iter()
            .map(|(part_id, parts)| {
                (
                    part_id,
                    parts
                        .into_iter()
                        .map(|part| part.into())
                        .collect::<Vec<Arc<MicroPartition>>>(),
                )
            })
            .collect();
        let out = py.allow_threads(|| run_local(&self.local_physical_plan, native_psets))?;
        let iter = Box::new(out.map(|part| {
            part.map(|p| pyo3::Python::with_gil(|py| PyMicroPartition::from(p).into_py(py)))
        }));
        let part_iter = LocalPartitionIterator { iter };
        Ok(part_iter.into_py(py))
    }
}

pub fn run_local(
    physical_plan: &LocalPhysicalPlan,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let final_schema = physical_plan.schema();
    let mut pipeline = physical_plan_to_pipeline(physical_plan, &psets)?;
    let (tx, rx) = create_single_channel(1);

    let handle = std::thread::spawn(move || {
        refresh_chrome_trace();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(10)
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("Executor-Worker-{}", id)
            })
            .build()
            .expect("Failed to create tokio runtime");

        runtime.block_on(async {
            let (sender, mut receiver) = create_channel(*NUM_CPUS, true);
            let mut runtime_handle = ExecutionRuntimeHandle::default();
            pipeline.start(sender, &mut runtime_handle).await?;

            while let Some(part) = receiver.recv().await {
                let _ = tx.send(part).await;
            }

            while let Some(result) = runtime_handle.join_next().await {
                match result {
                    Ok(Err(e)) => {
                        runtime_handle.shutdown().await;
                        return DaftResult::Err(e);
                    }
                    Err(e) => {
                        runtime_handle.shutdown().await;
                        return DaftResult::Err(Error::JoinError { source: e }.into());
                    }
                    _ => {}
                }
            }
            Ok(())
        })
    });

    struct ReceiverIterator {
        rx: SingleReceiver,
        handle: Option<std::thread::JoinHandle<DaftResult<()>>>,
        has_output: bool,
        schema: SchemaRef,
    }

    impl Iterator for ReceiverIterator {
        type Item = DaftResult<Arc<MicroPartition>>;

        fn next(&mut self) -> Option<Self::Item> {
            match self.rx.blocking_recv() {
                Some(part) => {
                    self.has_output = true;
                    Some(Ok(part))
                }
                None => {
                    if let Some(h) = self.handle.take() {
                        match h.join() {
                            Ok(Ok(())) => {
                                if self.has_output {
                                    None
                                } else {
                                    let empty_mp = MicroPartition::empty(Some(self.schema.clone()));
                                    Some(Ok(Arc::new(empty_mp)))
                                }
                            }
                            Ok(Err(e)) => Some(Err(e)),
                            Err(_) => Some(Err(DaftError::InternalError("Join error".to_string()))),
                        }
                    } else {
                        None
                    }
                }
            }
        }
    }

    Ok(Box::new(ReceiverIterator {
        rx,
        handle: Some(handle),
        has_output: false,
        schema: final_schema.clone(),
    }))
}
