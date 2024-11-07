use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_core::prelude::Schema;
use daft_logical_plan::{InMemoryInfo, LogicalPlan};
use daft_physical_plan::{AdaptivePlanner, MaterializedResults};
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig, daft_logical_plan::PyLogicalPlanBuilder,
    pyo3::prelude::*,
};

use crate::PhysicalPlanScheduler;
/// A work scheduler for physical plans.
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct AdaptivePhysicalPlanScheduler {
    planner: AdaptivePlanner,
}

impl AdaptivePhysicalPlanScheduler {
    #[must_use]
    pub fn new(logical_plan: Arc<LogicalPlan>, cfg: Arc<DaftExecutionConfig>) -> Self {
        Self {
            planner: AdaptivePlanner::new(logical_plan, cfg),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl AdaptivePhysicalPlanScheduler {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let logical_plan = logical_plan_builder.builder.build();
            Ok(Self::new(logical_plan, cfg.config.clone()))
        })
    }
    pub fn next(&mut self, py: Python) -> PyResult<(Option<usize>, PhysicalPlanScheduler)> {
        py.allow_threads(|| {
            let output = self.planner.next_stage()?;
            let sid = output.source_id();
            Ok((sid, output.into()))
        })
    }

    pub fn is_done(&self) -> PyResult<bool> {
        Ok(self.planner.is_done())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &mut self,
        source_id: usize,
        partition_key: &str,
        cache_entry: PyObject,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
        py: Python,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            let in_memory_info = InMemoryInfo::new(
                Schema::empty().into(), // TODO thread in schema from in memory scan
                partition_key.into(),
                cache_entry,
                num_partitions,
                size_bytes,
                num_rows,
                None, // TODO(sammy) thread through clustering spec to Python
            );

            self.planner.update(MaterializedResults {
                source_id,
                in_memory_info,
            })?;
            Ok(())
        })
    }
}
