use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::{
    ops::{Limit as LogicalLimit, Offset as LogicalOffset},
    LogicalPlan,
};

/// This rule optimizes Limit operators by:
/// 1. Combine two adjacent `Limit` nodes into one by merging their expressions
/// 2. Merge offset and limit value into `Limit` node, and pushes down `Limit` through `Offset`
#[derive(Default, Debug)]
pub struct EliminateLimits {}

impl EliminateLimits {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateLimits {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

impl EliminateLimits {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Limit(LogicalLimit {
                input,
                offset,
                limit,
                eager,
                ..
            }) if offset.is_none() => {
                let limit = limit.unwrap() as usize;
                match input.as_ref() {
                    // Fold Limit together.
                    //
                    // Limit-Limit -> Limit
                    LogicalPlan::Limit(LogicalLimit {
                        input,
                        offset: child_offset,
                        limit: child_limit,
                        eager: child_eager,
                        ..
                    }) if child_offset.is_none() => {
                        let new_limit = limit.min(child_limit.unwrap() as usize);
                        let new_eager = eager | child_eager;

                        let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::try_new(
                            input.clone(),
                            None,
                            Some(new_limit as u64),
                            new_eager,
                        )?));
                        // we rerun the optimizer, ideally when we move to a visitor pattern this should go away
                        let optimized = self
                            .try_optimize_node(new_plan.clone())?
                            .or(Transformed::yes(new_plan))
                            .data;
                        Ok(Transformed::yes(optimized))
                    }
                    // Merge offset and limit value into Limit node, and pushes down Limit through Offset
                    //
                    // Limit(x)-Offset(y) -> Offset(y)-Limit(x + y)
                    LogicalPlan::Offset(LogicalOffset { input, offset, .. }) => {
                        let limit = limit as u64 + *offset;
                        let new_limit = Arc::new(LogicalPlan::Limit(LogicalLimit::try_new(
                            input.clone(),
                            None,
                            Some(limit),
                            *eager,
                        )?));

                        let new_offset =
                            Arc::new(LogicalPlan::Offset(LogicalOffset::new(new_limit, *offset)));

                        Ok(Transformed::yes(new_offset))
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
    use daft_core::prelude::*;
    use rstest::rstest;

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::EliminateLimits,
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
                vec![Box::new(EliminateLimits::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    /// Tests that multiple adjacent Limits fold into the smallest limit.
    ///
    /// Limit[x]-Limit[y] -> Limit[min(x,y)]
    #[rstest]
    fn limit_folds_with_smaller_limit(
        #[values(false, true)] smaller_first: bool,
    ) -> DaftResult<()> {
        let smaller_limit = 5;
        let limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit(if smaller_first { smaller_limit } else { limit }, false)?
            .limit(if smaller_first { limit } else { smaller_limit }, false)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .limit(smaller_limit, false)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_push_limit_through_offset1() -> DaftResult<()> {
        let offset = 7;
        let limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .offset(offset)?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .limit(offset + limit, false)?
            .offset(offset)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_push_limit_through_offset2() -> DaftResult<()> {
        let offset = 7;
        let limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit(limit, false)?
            .offset(offset)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .limit(limit, false)?
            .offset(offset)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }
}
