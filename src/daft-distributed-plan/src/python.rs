use common_daft_config::PyDaftExecutionConfig;
use daft_local_plan::PyLocalPhysicalPlan;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::planner::DistributedPhysicalPlanner;

#[pyclass(module = "daft.daft")]
struct DistributedPhysicalPlan {
    planner: DistributedPhysicalPlanner,
}

#[pymethods]
impl DistributedPhysicalPlan {
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

    pub fn to_local_physical_plans(&mut self) -> PyResult<Vec<PyLocalPhysicalPlan>> {
        let plans = self.planner.to_local_physical_plans()?;
        Ok(plans
            .into_iter()
            .map(|plan| PyLocalPhysicalPlan { plan })
            .collect())
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<DistributedPhysicalPlan>()?;
    Ok(())
}
