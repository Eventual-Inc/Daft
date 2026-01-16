use std::sync::Arc;

use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_recordbatch::python::PyRecordBatch;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{ExecutionEngineFinalResult, LocalPhysicalPlanRef, translate};

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

#[pyclass(module = "daft.daft", name = "PyExecutionEngineFinalResult", frozen)]
pub struct PyExecutionEngineFinalResult {
    inner: Arc<ExecutionEngineFinalResult>,
}

#[pymethods]
impl PyExecutionEngineFinalResult {
    fn encode(&self) -> Vec<u8> {
        self.inner.encode()
    }

    fn to_recordbatch(&self) -> PyResult<PyRecordBatch> {
        Ok(self.inner.to_recordbatch()?.into())
    }
}

impl From<ExecutionEngineFinalResult> for PyExecutionEngineFinalResult {
    fn from(inner: ExecutionEngineFinalResult) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyExecutionEngineFinalResult>()?;
    Ok(())
}
