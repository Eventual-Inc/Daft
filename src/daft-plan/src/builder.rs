use std::sync::Arc;

use crate::logical_plan::LogicalPlan;
use crate::ops;
use crate::source_info::{FileFormat, FilesInfo, SourceInfo};

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
}

#[cfg(feature = "python")]
#[pymethods]
impl LogicalPlanBuilder {
    #[staticmethod]
    pub fn read_parquet(filepaths: Vec<String>, schema: &PySchema) -> PyResult<LogicalPlanBuilder> {
        let source_info = SourceInfo::FilesInfo(FilesInfo::new(
            FileFormat::Parquet,
            filepaths,
            schema.schema.clone(),
        ));
        let logical_plan_builder = LogicalPlanBuilder::from_source(ops::Source::new(
            schema.schema.clone(),
            source_info.into(),
        ));
        Ok(logical_plan_builder)
    }

    pub fn filter(&self, predicate: &PyExpr) -> PyResult<LogicalPlanBuilder> {
        let logical_plan_builder = LogicalPlanBuilder::from_filter(ops::Filter::new(
            predicate.expr.clone().into(),
            self.plan.clone(),
        ));
        Ok(logical_plan_builder)
    }

    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.plan.schema().into())
    }
}
