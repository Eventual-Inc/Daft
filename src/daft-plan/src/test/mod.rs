use std::sync::Arc;

use daft_core::{datatypes::Field, schema::Schema};
use daft_scan::{file_format::FileFormatConfig, storage_config::StorageConfig, Pushdowns};

use crate::{builder::LogicalPlanBuilder, source_info::FileInfos, NativeStorageConfig};

/// Create a dummy scan node containing the provided fields in its schema.
pub fn dummy_scan_node(fields: Vec<Field>) -> LogicalPlanBuilder {
    dummy_scan_node_with_pushdowns(fields, Default::default())
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_node_with_pushdowns(
    fields: Vec<Field>,
    pushdowns: Pushdowns,
) -> LogicalPlanBuilder {
    let schema = Arc::new(Schema::new(fields).unwrap());

    LogicalPlanBuilder::table_scan_with_pushdowns(
        FileInfos::new_internal(vec!["/foo".to_string()], vec![None], vec![None]),
        schema,
        FileFormatConfig::Json(Default::default()).into(),
        StorageConfig::Native(NativeStorageConfig::new_internal(true, None).into()).into(),
        pushdowns,
    )
    .unwrap()
}

pub fn dummy_scan_operator_node(fields: Vec<Field>) -> LogicalPlanBuilder {
    dummy_scan_operator_node_with_pushdowns(fields, Default::default())
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_operator_node_with_pushdowns(
    fields: Vec<Field>,
    pushdowns: Pushdowns,
) -> LogicalPlanBuilder {
    let schema = Arc::new(Schema::new(fields).unwrap());
    let anon = daft_scan::AnonymousScanOperator::new(
        vec!["/foo".to_string()],
        schema,
        FileFormatConfig::Json(Default::default()).into(),
        StorageConfig::Native(NativeStorageConfig::new_internal(true, None).into()).into(),
    );
    LogicalPlanBuilder::table_scan_with_scan_operator(
        daft_scan::ScanOperatorRef(Arc::new(anon)),
        Some(pushdowns),
    )
    .unwrap()
}
