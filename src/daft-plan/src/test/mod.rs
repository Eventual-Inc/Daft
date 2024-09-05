use std::sync::Arc;

use daft_core::{datatypes::Field, schema::Schema};
use daft_scan::{
    file_format::FileFormatConfig, storage_config::NativeStorageConfig,
    storage_config::StorageConfig, AnonymousScanOperator, Pushdowns, ScanOperator,
};

use crate::builder::LogicalPlanBuilder;

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
    LogicalPlanBuilder::table_scan(daft_scan::ScanOperatorRef(scan_op), Some(pushdowns)).unwrap()
}

#[macro_export]
macro_rules! assert_optimized_plan_with_batches_eq {
    ($type:ty, $plan:expr, $expected:expr, $batches:expr) => {
        use crate::optimizer::Optimizer;

        struct TestOptimizer {}
        impl Optimizer<$type> for TestOptimizer {
            fn batches(&self) -> impl IntoIterator<Item = RuleBatch<$type>> {
                $batches
            }
        }

        let optimizer = TestOptimizer {};
        let optimized_plan = optimizer.execute($plan.clone())?;

        assert_eq!(
            optimized_plan,
            $expected,
            "\n\nOptimized plan not equal to expected.\n\nBefore Optimization:\n{}\n\nOptimized:\n{}\n\nExpected:\n{}",
            $plan.repr_ascii(false),
            optimized_plan.repr_ascii(false),
            $expected.repr_ascii(false)
        );
    };
}
