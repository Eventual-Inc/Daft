use common_daft_config::PyDaftExecutionConfig;
use daft_local_plan::PyLocalPhysicalPlan;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::planner::DistributedPhysicalPlanner;

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlanner")]
struct PyDistributedPhysicalPlanner {
    planner: DistributedPhysicalPlanner,
}

#[pymethods]
impl PyDistributedPhysicalPlanner {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        builder: &PyLogicalPlanBuilder,
        config: &PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        let planner = DistributedPhysicalPlanner::from_logical_plan_builder(
            &builder.builder,
            &config.config,
        )?;
        Ok(Self { planner })
    }

    pub fn next_plan(&mut self) -> PyResult<Option<PyLocalPhysicalPlan>> {
        let plan = self.planner.next_plan()?;
        Ok(plan.map(|plan| PyLocalPhysicalPlan { plan }))
    }

    pub fn has_remaining_plans(&self) -> bool {
        self.planner.has_remaining_plans()
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlanner>()?;
    Ok(())
}
