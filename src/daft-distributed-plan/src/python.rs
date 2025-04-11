use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef, PyLocalPhysicalPlan};
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_scan::{python::pylib::PyScanTask, ScanTaskRef};
use pyo3::prelude::*;

use crate::planner::DistributedPhysicalPlanner;

#[pyclass(module = "daft.daft")]
struct DistributedPhysicalPlan {
    planner: DistributedPhysicalPlanner,
}

#[pymethods]
impl DistributedPhysicalPlan {
    #[staticmethod]
    pub fn from_logical_plan_builder(builder: &PyLogicalPlanBuilder) -> PyResult<Self> {
        let planner = DistributedPhysicalPlanner::from_logical_plan_builder(&builder.builder)?;
        Ok(Self { planner })
    }

    pub fn next_plan(&mut self) -> PyResult<Option<PyLocalPhysicalPlan>> {
        let next_plan = self.planner.next_plan()?;
        Ok(next_plan.map(|plan| PyLocalPhysicalPlan { plan }))
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<DistributedPhysicalPlan>()?;
    Ok(())
}
