use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_core::schema::Schema;

use crate::PhysicalPlanScheduler;
use daft_plan::InMemoryInfo;
use daft_plan::LogicalPlan;
use daft_plan::{AdaptivePlanner, MaterializedResults};

#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig, daft_plan::PyLogicalPlanBuilder, pyo3::prelude::*,
};
/// A work scheduler for physical plans.
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct AdaptivePhysicalPlanScheduler {
    planner: AdaptivePlanner,
}

impl AdaptivePhysicalPlanScheduler {
    pub fn new(logical_plan: Arc<LogicalPlan>, cfg: Arc<DaftExecutionConfig>) -> Self {
        AdaptivePhysicalPlanScheduler {
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
        py: Python<'_>,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let logical_plan = logical_plan_builder.builder.build();
            Ok(AdaptivePhysicalPlanScheduler::new(
                logical_plan,
                cfg.config.clone(),
            ))
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
        cache_entry: &PyAny,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
        py: Python,
    ) -> PyResult<()> {
        let cache_entry = cache_entry.into();
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
                in_memory_info,
                source_id,
            })?;
            Ok(())
        })
    }
}
