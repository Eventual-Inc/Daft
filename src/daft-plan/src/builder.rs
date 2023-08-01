use std::sync::Arc;

use crate::logical_plan::LogicalPlan;
use crate::{ops, source_info};

#[cfg(feature = "python")]
use daft_core::python::schema::PySchema;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg_attr(feature = "python", pyclass)]
pub struct LogicalPlanBuilder {
    _plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    // Create a new LogicalPlanBuilder for a Source node.
    pub fn from_source(source: ops::Source) -> Self {
        Self {
            _plan: LogicalPlan::Source(source).into(),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl LogicalPlanBuilder {
    #[staticmethod]
    pub fn source(filepaths: Vec<String>, schema: &PySchema) -> PyResult<LogicalPlanBuilder> {
        let source_info = source_info::SourceInfo::FilesInfo(source_info::FilesInfo::new(
            filepaths,
            schema.schema.clone(),
        ));
        let logical_plan_builder = LogicalPlanBuilder::from_source(ops::Source::new(
            schema.schema.clone(),
            source_info.into(),
        ));
        Ok(logical_plan_builder)
    }
}
