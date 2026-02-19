use std::sync::Arc;

use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_recordbatch::python::PyRecordBatch;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{ExecutionMetadata, LocalPhysicalPlanRef, translate};

#[pyclass(module = "daft.daft", name = "LocalPhysicalPlan")]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyLocalPhysicalPlan {
    pub plan: LocalPhysicalPlanRef,
}

#[pymethods]
impl PyLocalPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(logical_plan_builder: &PyLogicalPlanBuilder) -> PyResult<Self> {
        let logical_plan = logical_plan_builder.builder.build();
        let physical_plan = translate(&logical_plan)?;
        Ok(Self {
            plan: physical_plan,
        })
    }
}

impl_bincode_py_state_serialization!(PyLocalPhysicalPlan);

#[pyclass(module = "daft.daft", name = "PyExecutionMetadata", frozen)]
pub struct PyExecutionMetadata {
    inner: Arc<ExecutionMetadata>,
}

#[pymethods]
impl PyExecutionMetadata {
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

impl From<ExecutionMetadata> for PyExecutionMetadata {
    fn from(inner: ExecutionMetadata) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyExecutionMetadata>()?;
    Ok(())
}
