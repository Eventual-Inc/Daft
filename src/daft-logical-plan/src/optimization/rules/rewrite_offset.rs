use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode};

use crate::{
    ops::{Limit as LogicalLimit, Offset as LogicalOffset},
    optimization::rules::OptimizerRule,
    LogicalPlan,
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
                let offset = *offset;
                match input.as_ref() {
                    LogicalPlan::Offset(_) => Err(DaftError::InternalError(
                        "Adjacent OFFSET nodes should be merged before rewrite".to_string(),
                    )),
                    // Offset(x)-Limit(y) -> Limit(Some(x), Some(y))
                    LogicalPlan::Limit(LogicalLimit {
                        input: child_input,
                        offset: child_offset,
                        limit: Some(limit),
                        eager,
                        ..
                    }) => {
                        if child_offset.is_some() {
                            return Err(DaftError::InternalError(
                                "LIMIT node's offset field should be None before rewrite"
                                    .to_string(),
                            ));
                        }

                        // Eliminate Offset node if offset >= limit
                        let limit = *limit;
                        if offset >= limit {
                            let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::try_new(
                                child_input.clone(),
                                None,
                                Some(0),
                                *eager,
                            )?));
                            return Ok(Transformed::yes(new_plan));
                        }

                        let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::try_new(
                            child_input.clone(),
                            Some(offset),
                            Some(limit),
                            *eager,
                        )?));
                        Ok(Transformed::yes(new_plan))
                    }
                    // Offset(x) -> Limit(Some(x), None)
                    _ => {
                        let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::try_new(
                            input.clone(),
                            Some(offset),
                            None,
                            false,
                        )?));
                        Ok(Transformed::yes(new_plan))
                    }
                }
            }
            // Added validation for LIMIT nodes
            LogicalPlan::Limit(LogicalLimit { input, .. }) => match input.as_ref() {
                LogicalPlan::Limit(_) => Err(DaftError::InternalError(
                    "Adjacent LIMIT nodes should be merged before rewrite".to_string(),
                )),
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

    use common_error::DaftResult;
    use daft_schema::{dtype::DataType, field::Field};

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::RewriteOffset,
            test::{assert_optimized_plan_with_rules_eq, assert_optimized_plan_with_rules_err},
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
                vec![Box::new(RewriteOffset::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    fn assert_optimized_plan_err(plan: Arc<LogicalPlan>, expected_err_msg: &str) -> DaftResult<()> {
        assert_optimized_plan_with_rules_err(
            plan,
            expected_err_msg,
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
            "DaftError::InternalError Adjacent OFFSET nodes should be merged before rewrite",
        )
    }

    #[test]
    fn test_adjacent_limits() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit(7, false)?
            .limit(11, false)?
            .build();
        assert_optimized_plan_err(
            plan,
            "DaftError::InternalError Adjacent LIMIT nodes should be merged before rewrite",
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
            "DaftError::InternalError LIMIT node should be push through OFFSET node before rewrite",
        )
    }

    #[test]
    fn test_limit_with_offset() -> DaftResult<()> {
        let offset = 7;
        let limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit_with_offset(Some(offset), Some(limit), false)?
            .offset(offset)?
            .build();
        assert_optimized_plan_err(
            plan,
            "DaftError::InternalError LIMIT node's offset field should be None before rewrite",
        )
    }

    #[test]
    fn test_offset_gteq_limit() -> DaftResult<()> {
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
            .limit_with_offset(None, Some(0), false)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_merge_limit_offset() -> DaftResult<()> {
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
            .limit_with_offset(Some(7), Some(10), false)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn test_rewrite_offset() -> DaftResult<()> {
        let offset = 7;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone()).offset(offset)?.build();
        let expected = dummy_scan_node(scan_op)
            .limit_with_offset(Some(7), None, false)?
            .build();
        assert_optimized_plan_eq(plan, expected)
    }
}
