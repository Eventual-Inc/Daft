use std::{collections::HashMap, sync::Arc};

use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use daft_recordbatch::python::PyRecordBatch;
use pyo3::{prelude::*, types::PyDict};
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::{ExecutionStats, LocalPhysicalPlanRef, translate};
use crate::{FlightShufflePartitionRef, Input, LocalPhysicalPlan, RepartitionWriteBackend};

#[pyclass(
    module = "daft.daft",
    name = "ShuffleWriteInfo",
    frozen,
    from_py_object
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyShuffleWriteInfo {
    #[pyo3(get)]
    pub backend: String,
    #[pyo3(get)]
    pub shuffle_id: u64,
    #[pyo3(get)]
    pub num_partitions: usize,
}

#[pyclass(
    module = "daft.daft",
    name = "FlightShufflePartitionRef",
    frozen,
    from_py_object
)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyFlightShufflePartitionRef {
    pub inner: FlightShufflePartitionRef,
}

impl_bincode_py_state_serialization!(PyFlightShufflePartitionRef);

#[pymethods]
impl PyFlightShufflePartitionRef {
    #[new]
    pub fn new(
        shuffle_id: u64,
        server_address: String,
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    ) -> Self {
        Self {
            inner: FlightShufflePartitionRef {
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

impl From<FlightShufflePartitionRef> for PyFlightShufflePartitionRef {
    fn from(inner: FlightShufflePartitionRef) -> Self {
        Self { inner }
    }
}

impl From<PyFlightShufflePartitionRef> for FlightShufflePartitionRef {
    fn from(py_ref: PyFlightShufflePartitionRef) -> Self {
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

    fn shuffle_write_info(&self) -> Option<PyShuffleWriteInfo> {
        match self.plan.as_ref() {
            LocalPhysicalPlan::RepartitionWrite(repartition_write) => {
                match &repartition_write.backend {
                    RepartitionWriteBackend::Ray => Some(PyShuffleWriteInfo {
                        backend: "ray".to_string(),
                        shuffle_id: 0,
                        num_partitions: repartition_write.num_partitions,
                    }),
                    RepartitionWriteBackend::Flight { shuffle_id, .. } => {
                        Some(PyShuffleWriteInfo {
                            backend: "flight".to_string(),
                            shuffle_id: *shuffle_id,
                            num_partitions: repartition_write.num_partitions,
                        })
                    }
                }
            }
            _ => None,
        }
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
    parent.add_class::<PyFlightShufflePartitionRef>()?;
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyInput>()?;
    parent.add_class::<PyExecutionStats>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field, Schema};
    use daft_dsl::resolved_col;
    use daft_logical_plan::{
        partitioning::{HashRepartitionConfig, RepartitionSpec},
        stats::StatsState,
    };

    use super::PyLocalPhysicalPlan;
    use crate::{LocalNodeContext, LocalPhysicalPlan, RepartitionWriteBackend};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
    }

    #[test]
    fn shuffle_write_info_reports_repartition_write_backends() {
        let schema = test_schema();
        let input = LocalPhysicalPlan::in_memory_scan(
            0,
            schema.clone(),
            0,
            StatsState::NotMaterialized,
            LocalNodeContext::new(None),
        );
        let repartition_spec =
            RepartitionSpec::Hash(HashRepartitionConfig::new(Some(2), vec![resolved_col("a")]));

        let ray_plan = PyLocalPhysicalPlan {
            plan: LocalPhysicalPlan::repartition_write(
                input.clone(),
                2,
                schema.clone(),
                RepartitionWriteBackend::Ray,
                repartition_spec.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(None),
            ),
        };
        let ray_info = ray_plan.shuffle_write_info().expect("ray shuffle info");
        assert_eq!(ray_info.backend, "ray");
        assert_eq!(ray_info.shuffle_id, 0);
        assert_eq!(ray_info.num_partitions, 2);

        let flight_plan = PyLocalPhysicalPlan {
            plan: LocalPhysicalPlan::repartition_write(
                input,
                3,
                schema.clone(),
                RepartitionWriteBackend::Flight {
                    shuffle_id: 42,
                    shuffle_dirs: vec!["/tmp".to_string()],
                    compression: None,
                },
                repartition_spec,
                StatsState::NotMaterialized,
                LocalNodeContext::new(None),
            ),
        };
        let flight_info = flight_plan
            .shuffle_write_info()
            .expect("flight shuffle info");
        assert_eq!(flight_info.backend, "flight");
        assert_eq!(flight_info.shuffle_id, 42);
        assert_eq!(flight_info.num_partitions, 3);

        let non_shuffle_plan = PyLocalPhysicalPlan {
            plan: LocalPhysicalPlan::in_memory_scan(
                0,
                schema,
                0,
                StatsState::NotMaterialized,
                LocalNodeContext::new(None),
            ),
        };
        assert!(non_shuffle_plan.shuffle_write_info().is_none());
    }
}
