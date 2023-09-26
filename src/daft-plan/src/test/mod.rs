use std::sync::Arc;

use daft_core::{datatypes::Field, schema::Schema};

use crate::{
    builder::LogicalPlanBuilder,
    source_info::{FileFormatConfig, FileInfos},
    JsonSourceConfig,
};

/// Create a dummy scan node containing the provided fields in its schema.
pub fn dummy_scan_node(fields: Vec<Field>) -> LogicalPlanBuilder {
    let schema = Arc::new(Schema::new(fields).unwrap());
    LogicalPlanBuilder::table_scan(
        FileInfos::new_internal(vec!["/foo".to_string()], vec![None], vec![None]),
        schema,
        FileFormatConfig::Json(JsonSourceConfig {}).into(),
        Default::default(),
    )
    .unwrap()
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_node_with_limit(fields: Vec<Field>, limit: Option<usize>) -> LogicalPlanBuilder {
    let schema = Arc::new(Schema::new(fields).unwrap());
    LogicalPlanBuilder::table_scan_with_limit(
        FileInfos::new_internal(vec!["/foo".to_string()], vec![None], vec![None]),
        schema,
        FileFormatConfig::Json(JsonSourceConfig {}).into(),
        Default::default(),
        limit,
    )
    .unwrap()
}
