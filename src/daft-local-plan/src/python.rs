use std::{collections::HashMap, sync::Arc};

use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use daft_recordbatch::python::PyRecordBatch;
use pyo3::{prelude::*, types::PyDict};
use serde::{Deserialize, Serialize};

use crate::Input;
#[cfg(feature = "python")]
use crate::{ExecutionStats, LocalPhysicalPlanRef, translate};

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

    /// True iff a single task amplifies one input into multiple output MicroPartitions whose
    /// count is part of the plan's contract — repartitions, `IntoPartitions`, and `IntoBatches`.
    /// Coalescing such outputs collapses the slot/batch count and breaks every downstream
    /// operator that reasons about partition layout. Other operators (writes, IDs that encode a
    /// partition index, etc.) are safe because their per-task output rows are preserved by
    /// `concat`.
    fn has_partitioned_output(&self) -> bool {
        matches!(
            self.plan.as_ref(),
            crate::LocalPhysicalPlan::RepartitionWrite(_)
                | crate::LocalPhysicalPlan::GatherWrite(_)
                | crate::LocalPhysicalPlan::IntoPartitions(_)
                | crate::LocalPhysicalPlan::IntoBatches(_)
        )
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

    #[getter]
    pub fn skipped_corrupt_files(&self) -> Vec<(String, String, bool)> {
        self.inner.skipped_corrupt_files.clone()
    }

    #[getter]
    pub fn profile_telemetry(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use pyo3::types::{PyDict, PyDictMethods};

        let telemetry = &self.inner.profile_telemetry;
        let result = PyDict::new(py);
        result.set_item("peak_process_rss_bytes", telemetry.peak_process_rss_bytes)?;
        result.set_item("spilled_bytes", telemetry.spilled_bytes)?;

        let partitions = PyDict::new(py);
        for (node_id, stats) in &telemetry.partition_stats {
            let entry = PyDict::new(py);
            entry.set_item("count", stats.count)?;
            entry.set_item("total_rows", stats.total_rows)?;
            entry.set_item("max_rows", stats.max_rows)?;
            entry.set_item("total_bytes", stats.total_bytes)?;
            entry.set_item("max_bytes", stats.max_bytes)?;
            partitions.set_item(*node_id, entry)?;
        }
        result.set_item("partition_stats", partitions)?;

        let pushdowns = PyDict::new(py);
        for (node_id, stats) in &telemetry.scan_pushdowns {
            let entry = PyDict::new(py);
            entry.set_item("filter_requested", stats.filter_requested)?;
            entry.set_item("filter_applied", stats.filter_applied)?;
            entry.set_item("projection_requested", stats.projection_requested)?;
            entry.set_item("projection_applied", stats.projection_applied)?;
            pushdowns.set_item(*node_id, entry)?;
        }
        result.set_item("scan_pushdowns", pushdowns)?;
        Ok(result.into_any().unbind())
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
