use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{LocalPhysicalPlanRef, translate};

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

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    Ok(())
}
