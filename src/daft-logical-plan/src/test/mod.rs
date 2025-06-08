use std::sync::Arc;

use common_scan_info::{test::DummyScanOperator, Pushdowns, ScanOperatorRef};
use daft_schema::{field::Field, schema::Schema};

use crate::builder::LogicalPlanBuilder;

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_operator(fields: Vec<Field>) -> ScanOperatorRef {
    dummy_scan_operator_with_size(fields, None)
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit,
/// and with the provided size estimate.
pub fn dummy_scan_operator_with_size(
    fields: Vec<Field>,
    num_rows_per_task: Option<usize>,
) -> ScanOperatorRef {
    let schema = Arc::new(Schema::new(fields));
    ScanOperatorRef(Arc::new(DummyScanOperator {
        schema,
        num_scan_tasks: 1,
        num_rows_per_task,
    }))
}

/// Create a dummy scan node containing the provided fields in its schema.
pub fn dummy_scan_node(scan_op: ScanOperatorRef) -> LogicalPlanBuilder {
    dummy_scan_node_with_pushdowns(scan_op, Default::default())
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_node_with_pushdowns(
    scan_op: ScanOperatorRef,
    pushdowns: Pushdowns,
) -> LogicalPlanBuilder {
    LogicalPlanBuilder::table_scan(scan_op, Some(pushdowns)).unwrap()
}
