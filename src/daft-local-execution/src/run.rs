use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{translate, LocalPhysicalPlan};

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

pub fn run_local(
    physical_plan: &LocalPhysicalPlan,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
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
