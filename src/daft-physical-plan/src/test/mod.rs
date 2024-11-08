use std::sync::Arc;

use common_file_formats::FileFormatConfig;
use common_scan_info::{Pushdowns, ScanOperator};
use daft_logical_plan::builder::LogicalPlanBuilder;
use daft_scan::{
    storage_config::{NativeStorageConfig, StorageConfig},
    AnonymousScanOperator,
};
use daft_schema::{field::Field, schema::Schema};

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_operator(fields: Vec<Field>) -> Arc<dyn ScanOperator> {
    let schema = Arc::new(Schema::new(fields).unwrap());
    Arc::new(AnonymousScanOperator::new(
        vec!["/foo".to_string()],
        schema,
        FileFormatConfig::Json(Default::default()).into(),
        StorageConfig::Native(NativeStorageConfig::new_internal(true, None).into()).into(),
    ))
}

/// Create a dummy scan node containing the provided fields in its schema.
pub fn dummy_scan_node(scan_op: Arc<dyn ScanOperator>) -> LogicalPlanBuilder {
    dummy_scan_node_with_pushdowns(scan_op, Default::default())
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_node_with_pushdowns(
    scan_op: Arc<dyn ScanOperator>,
    pushdowns: Pushdowns,
) -> LogicalPlanBuilder {
    daft_scan::builder::table_scan(common_scan_info::ScanOperatorRef(scan_op), Some(pushdowns))
        .unwrap()
}
