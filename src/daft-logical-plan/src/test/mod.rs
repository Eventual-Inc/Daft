use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::{
    BoxScanTaskLikeIter, PartitionField, PhysicalScanInfo, Pushdowns, ScanOperator, ScanOperatorRef,
};
use daft_core::prelude::SchemaRef;
use daft_schema::{field::Field, schema::Schema};

use crate::{builder::LogicalPlanBuilder, ops::Source, SourceInfo};

#[derive(Debug)]
struct DummyScanOperator {
    pub schema: SchemaRef,
}

impl ScanOperator for DummyScanOperator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }

    fn can_absorb_select(&self) -> bool {
        false
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["DummyScanOperator".to_string()]
    }

    fn to_scan_tasks(&self, _: Pushdowns) -> DaftResult<BoxScanTaskLikeIter> {
        unimplemented!("Dummy scan operator cannot be turned into scan tasks")
    }
}

/// Create a dummy scan node containing the provided fields in its schema and the provided limit.
pub fn dummy_scan_operator(fields: Vec<Field>) -> ScanOperatorRef {
    let schema = Arc::new(Schema::new(fields).unwrap());
    ScanOperatorRef(Arc::new(DummyScanOperator { schema }))
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
    let schema = scan_op.0.schema();

    let schema_with_generated_fields = {
        if let Some(generated_fields) = scan_op.0.generated_fields() {
            // We use the non-distinct union here because some scan operators have table schema information that
            // already contain partitioned fields. For example,the deltalake scan operator takes the table schema.
            Arc::new(schema.non_distinct_union(&generated_fields))
        } else {
            schema.clone()
        }
    };
    // If column selection (projection) pushdown is specified, prune unselected columns from the schema.
    let output_schema = if let Pushdowns {
        columns: Some(columns),
        ..
    } = &pushdowns
        && columns.len() < schema_with_generated_fields.fields.len()
    {
        let pruned_upstream_schema = schema_with_generated_fields
            .fields
            .iter()
            .filter(|&(name, _)| columns.contains(name))
            .map(|(_, field)| field.clone())
            .collect::<Vec<_>>();
        Arc::new(Schema::new(pruned_upstream_schema).unwrap())
    } else {
        schema_with_generated_fields
    };

    LogicalPlanBuilder::new(
        Source::new(
            output_schema,
            SourceInfo::Physical(PhysicalScanInfo::new(
                scan_op.clone(),
                schema,
                scan_op.0.partitioning_keys().into(),
                pushdowns,
            ))
            .into(),
        )
        .into(),
        None,
    )
}
