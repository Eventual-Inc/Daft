use std::sync::Arc;

use daft_core::schema::SchemaRef;

use crate::logical_plan::LogicalPlan;
use crate::ops;
use crate::source_info;

#[derive(Clone)]
pub struct LogicalPlanBuilder {
    _plan: Arc<LogicalPlan>,
}

// Create a new LogicalPlanBuilder for a Source node.
pub fn new_plan_from_source(filepaths: Vec<String>, schema: SchemaRef) -> LogicalPlanBuilder {
    let source_info = source_info::SourceInfo::FilesInfo(source_info::FilesInfo {
        filepaths,
        schema: schema.clone(),
    });
    let source_node = LogicalPlan::Source(ops::Source {
        schema,
        source_info: source_info.into(),
        filters: vec![], // Will be populated by plan optimizer.
        limit: None,     // Will be populated by plan optimizer.
    });
    LogicalPlanBuilder {
        _plan: source_node.into(),
    }
}
