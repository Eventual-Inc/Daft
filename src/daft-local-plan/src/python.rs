use std::{any::Any, collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef, RayPartitionRef};
use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::{MicroPartition, MicroPartitionRef, python::PyMicroPartition};
use daft_recordbatch::python::PyRecordBatch;
use futures::FutureExt;
use pyo3::{prelude::*, types::PyDict};
use serde::{Deserialize, Serialize};

use crate::Input;
#[cfg(feature = "python")]
use crate::{ExecutionStats, LocalPhysicalPlanRef, translate};

#[derive(Debug, Clone)]
struct LazyRayPartition(RayPartitionRef);

impl Partition for LazyRayPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn size_bytes(&self) -> usize {
        self.0.size_bytes
    }
    fn num_rows(&self) -> usize {
        self.0.num_rows
    }
    fn fetch_native(
        &self,
    ) -> futures::future::BoxFuture<'static, DaftResult<Arc<dyn Any + Send + Sync>>> {
        let object_ref = self.0.object_ref.clone();
        async move {
            // First, await the ray.ObjectRef to get the resolved Python object.
            let resolved: pyo3::Py<pyo3::PyAny> =
                common_runtime::python::execute_python_coroutine::<_, pyo3::Py<pyo3::PyAny>>(
                    move |py| Ok(object_ref.bind(py).clone()),
                )
                .await?;
            // Then extract PyMicroPartition from the resolved object.
            // It may be a PyMicroPartition directly, or a Python MicroPartition wrapper.
            let py_mp: PyMicroPartition =
                pyo3::Python::attach(|py| -> pyo3::PyResult<PyMicroPartition> {
                    let obj = resolved.bind(py);
                    if let Ok(py_mp) = obj.extract::<PyMicroPartition>() {
                        Ok(py_mp)
                    } else {
                        Ok(obj
                            .getattr("_micropartition")?
                            .extract::<PyMicroPartition>()?)
                    }
                })
                .map_err(common_error::DaftError::from)?;
            Ok(py_mp.inner as Arc<dyn Any + Send + Sync>)
        }
        .boxed()
    }
}

#[pyclass(module = "daft.daft", name = "LocalPhysicalPlan")]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyLocalPhysicalPlan {
    pub plan: LocalPhysicalPlanRef,
}

#[pymethods]
impl PyLocalPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(
        py: Python<'_>,
        logical_plan_builder: &PyLogicalPlanBuilder,
        psets: HashMap<String, Vec<PyMicroPartition>>,
    ) -> PyResult<(Self, Py<PyDict>)> {
        let psets_mp: HashMap<String, Vec<MicroPartitionRef>> = psets
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|p| p.inner).collect()))
            .collect();
        let logical_plan = logical_plan_builder.builder.build();
        let (physical_plan, inputs) = translate(&logical_plan, &psets_mp)?;

        let dict = PyDict::new(py);
        for (source_id, input) in inputs {
            match &input {
                Input::InMemory(partitions) => {
                    let py_parts: Vec<PyMicroPartition> = partitions
                        .iter()
                        .map(|p| {
                            let mp = p
                                .as_any()
                                .downcast_ref::<MicroPartition>()
                                .expect("Partition must be MicroPartition in InMemory input");
                            PyMicroPartition::from(mp.clone())
                        })
                        .collect();
                    dict.set_item(source_id, py_parts)?;
                }
                _ => {
                    let py_input = PyInput { inner: input };
                    dict.set_item(source_id, py_input.into_pyobject(py)?)?;
                }
            }
        }

        Ok((
            Self {
                plan: physical_plan,
            },
            dict.into(),
        ))
    }
}

impl_bincode_py_state_serialization!(PyLocalPhysicalPlan);

#[pyclass(module = "daft.daft", name = "Input", frozen)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyInput {
    pub inner: Input,
}

impl_bincode_py_state_serialization!(PyInput);

impl<'py> FromPyObject<'_, 'py> for Input {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(py_input) = ob.extract::<PyInput>() {
            Ok(py_input.inner)
        } else if let Ok(partitions) = ob.extract::<Vec<PyMicroPartition>>() {
            Ok(Self::InMemory(
                partitions
                    .into_iter()
                    .map(|p| Arc::new(p.inner) as PartitionRef)
                    .collect(),
            ))
        } else if let Ok(partitions) = ob.extract::<Vec<common_partitioning::RayPartitionRef>>() {
            Ok(Self::InMemory(
                partitions
                    .into_iter()
                    .map(|p| Arc::new(LazyRayPartition(p)) as PartitionRef)
                    .collect(),
            ))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected Input, list[MicroPartition] or list[RayPartitionRef]",
            ))
        }
    }
}

#[pyclass(module = "daft.daft", name = "PyExecutionStats", frozen)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyExecutionStats {
    inner: Arc<ExecutionStats>,
}

impl_bincode_py_state_serialization!(PyExecutionStats);

#[pymethods]
impl PyExecutionStats {
    #[getter]
    pub fn query_id(&self) -> String {
        self.inner.query_id.to_string()
    }

    #[getter]
    pub fn query_plan(&self) -> Option<String> {
        self.inner
            .query_plan
            .clone()
            .map(|plan| serde_json::to_string(&plan).expect("Failed to serialize query plan"))
    }

    fn encode(&self) -> Vec<u8> {
        self.inner.encode()
    }

    fn to_recordbatch(&self) -> PyResult<PyRecordBatch> {
        Ok(self.inner.to_recordbatch()?.into())
    }
}

impl From<ExecutionStats> for PyExecutionStats {
    fn from(inner: ExecutionStats) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyInput>()?;
    parent.add_class::<PyExecutionStats>()?;
    Ok(())
}
