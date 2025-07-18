use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use crate::{ops::Offset as LogicalOffset, optimization::rules::OptimizerRule, LogicalPlan};

/// This rule optimizes Offset operators by:
/// 1. Eliminate `Offset` node if offset == 0
/// 2. Combine two adjacent `Offset` nodes into one by merging their expressions
#[derive(Default, Debug)]
pub struct EliminateOffsets {}

impl EliminateOffsets {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateOffsets {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl EliminateOffsets {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            // Eliminate Offset node if offset = 0
            LogicalPlan::Offset(LogicalOffset { input, offset, .. }) if *offset == 0 => {
                let new_plan = input.clone();
                let optimized = self
                    .try_optimize_node(new_plan.clone())?
                    .or(Transformed::yes(new_plan))
                    .data;
                Ok(Transformed::yes(optimized))
            }
            LogicalPlan::Offset(LogicalOffset { input, offset, .. }) => {
                let offset = *offset;
                match input.as_ref() {
                    // Merge adjacent Offset nodes
                    LogicalPlan::Offset(LogicalOffset {
                        input: child_input,
                        offset: child_offset,
                        ..
                    }) => {
                        let new_offset = offset + child_offset;
                        let new_plan = Arc::new(LogicalPlan::Offset(LogicalOffset::new(
                            child_input.clone(),
                            new_offset,
                        )));
                        let optimized = self
                            .try_optimize_node(new_plan.clone())?
                            .or(Transformed::yes(new_plan))
                            .data;
                        Ok(Transformed::yes(optimized))
                    }
                    _ => Ok(Transformed::no(plan)),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_schema::{dtype::DataType, field::Field};

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::EliminateOffsets,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(EliminateOffsets::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn test_eliminate_single_unnecessary_offset() -> DaftResult<()> {
        let limit = 7;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .offset(0)?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node(scan_op).limit(limit, false)?.build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_eliminate_multi_unnecessary_offsets() -> DaftResult<()> {
        let limit = 7;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .offset(0)?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node(scan_op).limit(limit, false)?.build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_merge_adjacent_offsets() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .offset(0)?
            .offset(1)?
            .offset(0)?
            .offset(2)?
            .offset(0)?
            .offset(3)?
            .offset(0)?
            .offset(4)?
            .offset(0)?
            .offset(5)?
            .build();
        let expected = dummy_scan_node(scan_op).offset(15)?.build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_merge_adjacent_offsets2() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        // 3..127 -> 6..127 -> 6..37 -> 15..37
        let plan = dummy_scan_node(scan_op.clone())
            .offset(0)?
            .offset(1)?
            .offset(0)?
            .offset(2)?
            .limit(127, false)?
            .offset(0)?
            .offset(3)?
            .offset(0)?
            .limit(31, false)?
            .offset(4)?
            .offset(0)?
            .offset(5)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .offset(3)?
            .limit(127, false)?
            .offset(3)?
            .limit(31, false)?
            .offset(9)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }
}
