use std::{any::Any, collections::HashMap, sync::Arc};

use common_partitioning::Partition;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use daft_recordbatch::python::PyRecordBatch;
use pyo3::{prelude::*, types::PyDict};
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::{ExecutionStats, LocalPhysicalPlanRef, translate};
use crate::{FlightPartitionRef, Input};

#[pyclass(
    module = "daft.daft",
    name = "FlightPartitionRef",
    frozen,
    from_py_object
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyFlightPartitionRef {
    pub inner: FlightPartitionRef,
}

impl Partition for PyFlightPartitionRef {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn size_bytes(&self) -> usize {
        self.inner.size_bytes
    }
    fn num_rows(&self) -> usize {
        self.inner.num_rows
    }
}

impl_bincode_py_state_serialization!(PyFlightPartitionRef);

#[pymethods]
impl PyFlightPartitionRef {
    #[new]
    pub fn new(
        shuffle_id: u64,
        server_address: String,
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    ) -> Self {
        Self {
            inner: FlightPartitionRef {
                shuffle_id,
                server_address,
                partition_ref_id,
                num_rows,
                size_bytes,
            },
        }
    }

    #[getter]
    pub fn shuffle_id(&self) -> u64 {
        self.inner.shuffle_id
    }

    #[getter]
    pub fn server_address(&self) -> String {
        self.inner.server_address.clone()
    }

    #[getter]
    pub fn partition_ref_id(&self) -> u64 {
        self.inner.partition_ref_id
    }

    #[getter]
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows
    }

    #[getter]
    pub fn size_bytes(&self) -> usize {
        self.inner.size_bytes
    }
}

impl From<FlightPartitionRef> for PyFlightPartitionRef {
    fn from(inner: FlightPartitionRef) -> Self {
        Self { inner }
    }
}

impl From<PyFlightPartitionRef> for FlightPartitionRef {
    fn from(py_ref: PyFlightPartitionRef) -> Self {
        py_ref.inner
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
                        .map(|p| PyMicroPartition::from(p.clone()))
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

#[pyclass(module = "daft.daft", name = "Input", frozen, from_py_object)]
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
                partitions.into_iter().map(|p| p.inner).collect(),
            ))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected Input or list[MicroPartition]",
            ))
        }
    }
}

#[pyclass(
    module = "daft.daft",
    name = "PyExecutionStats",
    frozen,
    from_py_object
)]
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
    parent.add_class::<PyFlightPartitionRef>()?;
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyInput>()?;
    parent.add_class::<PyExecutionStats>()?;
    Ok(())
}
