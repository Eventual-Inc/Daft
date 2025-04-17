use common_daft_config::PyDaftExecutionConfig;
use daft_local_plan::PyLocalPhysicalPlan;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::{planner::DistributedPhysicalPlanner, stage::SwordfishStage};

#[pyclass(module = "daft.daft", name = "SwordfishStage")]
struct PySwordfishStage {
    stage: Box<dyn SwordfishStage + Send + Sync>,
}

#[pymethods]
impl PySwordfishStage {
    pub fn next_plan(&mut self) -> PyResult<Option<PyLocalPhysicalPlan>> {
        let plan = self.stage.next_plan()?;
        Ok(plan.map(|plan| PyLocalPhysicalPlan { plan }))
    }

    pub fn is_done(&self) -> bool {
        self.stage.is_done()
    }

    pub fn update(&mut self, num_rows: usize) -> PyResult<()> {
        self.stage.update(num_rows)?;
        Ok(())
    }
}

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

    pub fn next_stage(&mut self) -> PyResult<Option<PySwordfishStage>> {
        let stage = self.planner.next_stage()?;
        Ok(stage.map(|stage| PySwordfishStage { stage }))
    }

    pub fn is_done(&self) -> bool {
        self.planner.is_done()
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlanner>()?;
    parent.add_class::<PySwordfishStage>()?;
    Ok(())
}
