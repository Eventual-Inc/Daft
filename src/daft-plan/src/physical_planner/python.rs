use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_core::schema::Schema;
use serde::{Deserialize, Serialize};

use pyo3::prelude::*;
use crate::LogicalPlan;
use crate::{physical_planner::planner::AdaptivePlanner, PhysicalPlanScheduler};
use daft_core::python::schema::PySchema;
use crate::source_info::InMemoryInfo;
use crate::physical_planner::planner::MaterializedResults;
/// A work scheduler for physical plans.
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct AdaptivePhysicalPlanScheduler {
    planner: AdaptivePlanner,
}

impl AdaptivePhysicalPlanScheduler {
    pub fn new(logical_plan: Arc<LogicalPlan>, cfg: Arc<DaftExecutionConfig>) -> Self {
        AdaptivePhysicalPlanScheduler {planner: AdaptivePlanner::new(logical_plan, cfg)}
    }
}



#[cfg(feature = "python")]
#[pymethods]
impl AdaptivePhysicalPlanScheduler {

    pub fn next(&mut self) -> PyResult<PhysicalPlanScheduler> {
        let output = self.planner.next()?;
        Ok(output.unwrap().into())
    }
    
    pub fn is_done(&self) -> PyResult<bool> {
        Ok(self.planner.is_done())
    }

    pub fn update(&mut self,
        partition_key: &str,
        cache_entry: &PyAny,
        num_partitions: usize,
        size_bytes: usize,
    ) -> PyResult<()> {
        let in_memory_info = InMemoryInfo::new(
            Schema::empty().into(), // TODO thread in schema from in memory scan
            partition_key.into(),
            cache_entry.into(),
            num_partitions,
            size_bytes,
            None, // TODO(sammy) thread through clustering spec to Python
        );

        self.planner.update(MaterializedResults {in_memory_info })?;
        Ok(())
    }
}