use std::sync::Arc;

use common_scan_info::{test::DummyScanOperator, Pushdowns, ScanOperatorRef};
use daft_schema::{field::Field, schema::Schema};

use crate::builder::LogicalPlanBuilder;

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_operator(fields: Vec<Field>) -> ScanOperatorRef {
    let schema = Arc::new(Schema::new(fields).unwrap());
    ScanOperatorRef(Arc::new(DummyScanOperator {
        schema,
        num_scan_tasks: 0,
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
