use std::sync::Arc;

use crate::logical_plan::LogicalPlan;
use crate::planner::PhysicalPlanner;
use crate::source_info::{FileInfo, PyFileFormatConfig, SourceInfo};
use crate::{ops, PartitionScheme, PartitionSpec};

#[cfg(feature = "python")]
use {daft_core::python::schema::PySchema, daft_dsl::python::PyExpr, pyo3::prelude::*};

#[cfg_attr(feature = "python", pyclass)]
#[derive(Debug)]
pub struct LogicalPlanBuilder {
    plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    // Create a new LogicalPlanBuilder for a Source node.
    pub fn from_source(source: ops::Source) -> Self {
        Self {
            plan: LogicalPlan::Source(source).into(),
        }
    }

    pub fn from_filter(filter: ops::Filter) -> Self {
        Self {
            plan: LogicalPlan::Filter(filter).into(),
        }
    }

    pub fn from_limit(limit: ops::Limit) -> Self {
        Self {
            plan: LogicalPlan::Limit(limit).into(),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl LogicalPlanBuilder {
    #[staticmethod]
    pub fn table_scan(
        file_paths: Vec<String>,
        schema: &PySchema,
        file_format_config: PyFileFormatConfig,
    ) -> PyResult<LogicalPlanBuilder> {
        let num_partitions = file_paths.len();
        let source_info = SourceInfo::new(
            schema.schema.clone(),
            FileInfo::new(file_paths, None, None, None),
            file_format_config.into(),
        );
        let partition_spec = PartitionSpec::new(PartitionScheme::Unknown, num_partitions, None);
        let logical_plan_builder = LogicalPlanBuilder::from_source(ops::Source::new(
            schema.schema.clone(),
            source_info.into(),
            partition_spec.into(),
        ));
        Ok(logical_plan_builder)
    }

    pub fn filter(&self, predicate: &PyExpr) -> PyResult<LogicalPlanBuilder> {
        let logical_plan_builder = LogicalPlanBuilder::from_filter(ops::Filter::new(
            predicate.expr.clone(),
            self.plan.clone(),
        ));
        Ok(logical_plan_builder)
    }

    pub fn limit(&self, limit: i64) -> PyResult<LogicalPlanBuilder> {
        let logical_plan_builder =
            LogicalPlanBuilder::from_limit(ops::Limit::new(limit, self.plan.clone()));
        Ok(logical_plan_builder)
    }

    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.plan.schema().into())
    }

    pub fn partition_spec(&self) -> PyResult<PartitionSpec> {
        Ok(self.plan.partition_spec().as_ref().clone())
    }

    pub fn to_partition_tasks(&self) -> PyResult<PyObject> {
        let planner = PhysicalPlanner {};
        let physical_plan = planner.plan(self.plan.as_ref())?;
        Python::with_gil(|py| physical_plan.to_partition_tasks(py))
    }

    pub fn repr_ascii(&self) -> PyResult<String> {
        Ok(self.plan.repr_ascii())
    }
}
