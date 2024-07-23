use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{translate, LocalPhysicalPlan};
use lazy_static::lazy_static;
use tracing::{event, instrument::WithSubscriber};
use tracing_chrome::{EventOrSpan, FlushGuard};
use tracing_subscriber::util::SubscriberInitExt;

#[cfg(feature = "python")]
use {
    daft_micropartition::python::PyMicroPartition,
    daft_plan::PyLogicalPlanBuilder,
    pyo3::{pyclass, pymethods, IntoPy, PyObject, PyRef, PyRefMut, PyResult, Python},
};

use crate::{channel::create_channel, pipeline::physical_plan_to_pipeline};

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

lazy_static! {
    static ref CHROME_GUARD_HANDLE: Mutex<Option<tracing_chrome::FlushGuard>> = Mutex::new(None);
}

pub fn run_local(
    physical_plan: &LocalPhysicalPlan,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    use tracing_chrome::ChromeLayerBuilder;
    use tracing_subscriber::{prelude::*, registry::Registry};

    {
        let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
        if let Some(fg) = mg.as_mut() {
            fg.start_new(None);
        } else {
            let (chrome_layer, _guard) = ChromeLayerBuilder::new()
                .trace_style(tracing_chrome::TraceStyle::Threaded)
                .build();
            tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(chrome_layer),
            )
            .unwrap();
            *mg = Some(_guard);
        }
    };

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

    let res = runtime.block_on(async {
        let pipeline = physical_plan_to_pipeline(physical_plan, &psets);
        let (sender, mut receiver) = create_channel(1, true);
        pipeline.start(sender);
        let mut result = vec![];
        while let Some(val) = receiver.recv().await {
            result.push(val);
        }
        result.into_iter()
    });
    Ok(Box::new(res))
}
