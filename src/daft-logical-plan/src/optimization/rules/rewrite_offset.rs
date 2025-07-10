use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode};

use crate::{
    ops::{Limit as LogicalLimit, Offset as LogicalOffset, Slice as LogicalSlice},
    optimization::rules::OptimizerRule,
    LogicalPlan,
};

/// This rule is used to converting `offset` to `project + filter + row_number` to implement offset
/// semantics. Transforms example:
/// ```text
/// - Offset (row count = x)
///    - ...
/// ```
///
/// Into:
/// ```text
/// - Project (prune rowNumber symbol)
///    - Filter (rowNumber > x)
///       - RowNumber
///          - ...
/// ```
#[derive(Default, Debug)]
pub struct RewriteOffset {}

impl RewriteOffset {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RewriteOffset {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

// FIXME rename by zhenchao 2025-07-10 20:58:23
impl RewriteOffset {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Offset(LogicalOffset { input, offset, .. }) => {
                let offset = *offset;
                match input.as_ref() {
                    LogicalPlan::Offset(_) => Err(DaftError::InternalError(
                        "Adjacent OFFSET nodes should have been merged before.".to_string(),
                    )),
                    // Offset(x)-Limit(y) -> Slice(Some(x), Some(y))
                    LogicalPlan::Limit(LogicalLimit {
                        input: limit_input,
                        limit,
                        eager,
                        ..
                    }) => {
                        let new_plan = Arc::new(LogicalPlan::Slice(LogicalSlice::try_new(
                            limit_input.clone(),
                            Some(offset),
                            Some(*limit),
                            *eager,
                        )?));
                        Ok(Transformed::yes(new_plan))
                    }
                    // Offset(x) -> Slice(Some(x), None)
                    _ => {
                        let new_plan = Arc::new(LogicalPlan::Slice(LogicalSlice::try_new(
                            input.clone(),
                            Some(offset),
                            None,
                            false,
                        )?));
                        Ok(Transformed::yes(new_plan))
                    }
                }
            }
            // Limit(x) -> Slice(None, Some(x))
            LogicalPlan::Limit(LogicalLimit {
                input: limit_input,
                limit,
                eager,
                ..
            }) => match limit_input.as_ref() {
                LogicalPlan::Limit(_) => Err(DaftError::InternalError(
                    "Adjacent LIMIT nodes should have been merged before.".to_string(),
                )),
                LogicalPlan::Offset(_) => Err(DaftError::InternalError(
                    "OFFSET should be optimized to the parent node of LIMIT before..".to_string(),
                )),
                _ => {
                    let new_plan = Arc::new(LogicalPlan::Slice(LogicalSlice::try_new(
                        limit_input.clone(),
                        None,
                        Some(*limit),
                        *eager,
                    )?));
                    Ok(Transformed::yes(new_plan))
                }
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}
// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use common_error::DaftResult;
//     use daft_dsl::{lit, resolved_col};
//     use daft_schema::{dtype::DataType, field::Field};
//
//     use crate::{
//         optimization::{
//             optimizer::{RuleBatch, RuleExecutionStrategy},
//             rules::RewriteOffset,
//             test::assert_optimized_plan_with_rules_eq,
//         },
//         test::{dummy_scan_node, dummy_scan_operator},
//         LogicalPlan,
//     };
//
//     /// Helper that creates an optimizer with the RewriteOffset rule registered, optimizes
//     /// the provided plan with said optimizer, and compares the optimized plan with
//     /// the provided expected plan.
//     fn assert_optimized_plan_eq(
//         plan: Arc<LogicalPlan>,
//         expected: Arc<LogicalPlan>,
//     ) -> DaftResult<()> {
//         assert_optimized_plan_with_rules_eq(
//             plan,
//             expected,
//             vec![RuleBatch::new(
//                 vec![Box::new(RewriteOffset::new())],
//                 RuleExecutionStrategy::Once,
//             )],
//         )
//     }
//
//     #[test]
//     fn test_rewrite_offset() -> DaftResult<()> {
//         let offset = 7;
//         let scan_op = dummy_scan_operator(vec![
//             Field::new("a", DataType::Int64),
//             Field::new("b", DataType::Utf8),
//         ]);
//         let plan = dummy_scan_node(scan_op.clone()).offset(offset)?.build();
//         let expected = dummy_scan_node(scan_op)
//             .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
//             .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(offset)))?
//             .select(vec![resolved_col("a"), resolved_col("b")])?
//             .build();
//         assert_optimized_plan_eq(plan, expected)?;
//         Ok(())
//     }
//
//     #[test]
//     fn test_rewrite_offsets() -> DaftResult<()> {
//         let scan_op = dummy_scan_operator(vec![
//             Field::new("a", DataType::Int64),
//             Field::new("b", DataType::Utf8),
//         ]);
//         let plan = dummy_scan_node(scan_op.clone())
//             .offset(1)?
//             .offset(3)?
//             .offset(7)?
//             .build();
//         let expected = dummy_scan_node(scan_op)
//             .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
//             .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(1u64)))?
//             .select(vec![resolved_col("a"), resolved_col("b")])?
//             .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
//             .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(3u64)))?
//             .select(vec![resolved_col("a"), resolved_col("b")])?
//             .add_monotonically_increasing_id(Some(RewriteOffset::DEFAULT_COLUMN_NAME))?
//             .filter(resolved_col(RewriteOffset::DEFAULT_COLUMN_NAME).gt_eq(lit(7u64)))?
//             .select(vec![resolved_col("a"), resolved_col("b")])?
//             .build();
//         assert_optimized_plan_eq(plan, expected)?;
//         Ok(())
//     }
// }
