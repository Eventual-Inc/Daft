use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode};

use crate::{
    LogicalPlan,
    ops::{Limit as LogicalLimit, Offset as LogicalOffset},
    optimization::rules::OptimizerRule,
};

/// This rule is mainly used to rewrite the `Offset` node into a `Limit` node.
/// The `Offset` node won't be passed to the physical plan layer.
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

impl RewriteOffset {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Offset(LogicalOffset { input, offset, .. }) => {
                match input.as_ref() {
                    // Adjacent OFFSET nodes should be merged before rewrite
                    LogicalPlan::Offset(_) => Err(DaftError::InternalError(
                        "Adjacent OFFSET nodes should be merged before rewrite".to_string(),
                    )),
                    // Offset(x)-Limit(y) -> Limit(y, Some(max(x - y, 0)))
                    LogicalPlan::Limit(LogicalLimit {
                        input: child_input,
                        limit,
                        offset: child_offset,
                        eager,
                        ..
                    }) => {
                        let offset = offset + child_offset.unwrap_or(0);
                        let limit = (*limit).saturating_sub(offset);
                        let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::new(
                            child_input.clone(),
                            limit,
                            Some(offset),
                            *eager,
                        )));
                        Ok(Transformed::yes(new_plan))
                    }
                    // Currently, offset without limit is not supported
                    _ => Err(DaftError::not_implemented(
                        // TODO(zhenchao) maybe we should allow users to use offset without limit
                        "Offset without limit is unsupported now!",
                    )),
                }
            }
            // Some checks on LIMIT nodes
            LogicalPlan::Limit(LogicalLimit { input, .. }) => match input.as_ref() {
                LogicalPlan::Offset(_) => Err(DaftError::InternalError(
                    "LIMIT node should be push through OFFSET node before rewrite".to_string(),
                )),
                _ => Ok(Transformed::no(plan)),
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::{DaftError, DaftResult};
    use daft_schema::{dtype::DataType, field::Field};
    use rstest::rstest;

    use crate::{
        LogicalPlan,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::RewriteOffset,
            test::{assert_optimized_plan_with_rules_eq, assert_optimized_plan_with_rules_err},
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    /// Helper that creates an optimizer with the RewriteOffset rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(RewriteOffset::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    fn assert_optimized_plan_err(plan: Arc<LogicalPlan>, err: DaftError) -> DaftResult<()> {
        assert_optimized_plan_with_rules_err(
            plan,
            err,
            vec![RuleBatch::new(
                vec![Box::new(RewriteOffset::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn test_adjacent_offsets() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .offset(7)?
            .offset(11)?
            .build();
        assert_optimized_plan_err(
            plan,
            DaftError::InternalError(
                "Adjacent OFFSET nodes should be merged before rewrite".to_string(),
            ),
        )
    }

    #[test]
    fn test_push_limit_through_offset() -> DaftResult<()> {
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
        assert_optimized_plan_err(
            plan,
            DaftError::InternalError(
                "LIMIT node should be push through OFFSET node before rewrite".to_string(),
            ),
        )
    }

    #[test]
    fn test_offset_greater_than_limit() -> DaftResult<()> {
        let offset = 11;
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
            .limit_with_offset(0, Some(11), false)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[rstest]
    fn test_merge_limit_offset(#[values(false, true)] none_offset: bool) -> DaftResult<()> {
        let offset = 7;
        let limit = 10;
        let offset_in_limit = 1;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit_with_offset(
                limit,
                if none_offset {
                    None
                } else {
                    Some(offset_in_limit)
                },
                false,
            )?
            .offset(offset)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .limit_with_offset(
                if none_offset {
                    limit - offset
                } else {
                    limit - offset - offset_in_limit
                },
                if none_offset {
                    Some(offset)
                } else {
                    Some(offset + offset_in_limit)
                },
                false,
            )?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_merge_limit_with_greater_offset() -> DaftResult<()> {
        let offset = 7;
        let limit = 10;
        let offset_in_limit = 5;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit_with_offset(limit, Some(offset_in_limit), false)?
            .offset(offset)?
            .build();
        let expected = dummy_scan_node(scan_op)
            .limit_with_offset(0, Some(offset + offset_in_limit), false)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_offset_without_limit() -> DaftResult<()> {
        let offset = 7;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone()).offset(offset)?.build();
        assert_optimized_plan_err(
            plan,
            DaftError::not_implemented("Offset without limit is unsupported now!"),
        )
    }
}
