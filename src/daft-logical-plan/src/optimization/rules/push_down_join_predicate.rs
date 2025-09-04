use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::join::{JoinSide, JoinType};

use super::OptimizerRule;
use crate::{
    LogicalPlan, LogicalPlanRef,
    ops::{Filter, Join},
};

/// Optimizer rule for pushing down join predicates that only need one side of the join.
///
/// # Example
/// In the following query, `a.y = 1` can be pushed down:
/// ```sql
/// -- before
/// SELECT * FROM a JOIN b ON a.x = b.x AND a.y = 1
/// -- after
/// SELECT * FROM (SELECT * FROM a WHERE a.y = 1) JOIN b ON a.x = b.x
/// ```
#[derive(Debug)]
pub struct PushDownJoinPredicate {}

impl PushDownJoinPredicate {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownJoinPredicate {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform(|node| {
            if let LogicalPlan::Join(Join {
                left,
                right,
                on,
                join_type,
                join_strategy,
                ..
            }) = node.as_ref()
            {
                let (can_push_down_left, can_push_down_right) = match join_type {
                    JoinType::Inner => (true, true),
                    JoinType::Left => (false, true),
                    JoinType::Right => (true, false),
                    JoinType::Outer => (false, false),
                    JoinType::Anti => (true, true),
                    JoinType::Semi => (true, true),
                };

                let mut new_on = on.clone();
                let mut left_pred = None;
                let mut right_pred = None;

                if can_push_down_left {
                    (new_on, left_pred) = new_on.split_side_only_preds(JoinSide::Left);
                }

                if can_push_down_right {
                    (new_on, right_pred) = new_on.split_side_only_preds(JoinSide::Right);
                }

                if left_pred.is_some() || right_pred.is_some() {
                    let new_left = if let Some(left_pred) = left_pred {
                        // Special logic for left-side of anti-join: since anti join removes rows based on the predicate, we need to negate it when pushing down
                        let left_pred = if *join_type == JoinType::Anti {
                            left_pred.not()
                        } else {
                            left_pred
                        };

                        Filter::try_new(left.clone(), left_pred)?.into()
                    } else {
                        left.clone()
                    };

                    let new_right = if let Some(right_pred) = right_pred {
                        Filter::try_new(right.clone(), right_pred)?.into()
                    } else {
                        right.clone()
                    };

                    let new_join =
                        Join::try_new(new_left, new_right, new_on, *join_type, *join_strategy)?
                            .into();

                    return Ok(Transformed::yes(new_join));
                }
            }

            Ok(Transformed::no(node))
        })
    }
}

#[cfg(test)]
mod tests {
    use daft_dsl::{ExprRef, left_col, resolved_col, right_col};
    use daft_schema::{dtype::DataType, field::Field};
    use rstest::rstest;

    use super::*;
    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    fn assert_optimized_plan_eq(plan: LogicalPlanRef, expected: LogicalPlanRef) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(PushDownJoinPredicate::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[rstest]
    #[case(
        JoinType::Inner,
        None,
        Some(resolved_col("a")),
        Some(resolved_col("b"))
    )]
    #[case(
        JoinType::Left,
        Some(left_col(Field::new("a", DataType::Boolean))),
        None,
        Some(resolved_col("b"))
    )]
    #[case(
        JoinType::Right,
        Some(right_col(Field::new("b", DataType::Boolean))),
        Some(resolved_col("a")),
        None
    )]
    #[case(
        JoinType::Outer,
        Some(left_col(Field::new("a", DataType::Boolean)).and(right_col(Field::new("b", DataType::Boolean)))),
        None,
        None,
    )]
    #[case(
        JoinType::Anti,
        None,
        Some(resolved_col("a").not()),
        Some(resolved_col("b"))
    )]
    #[case(JoinType::Semi, None, Some(resolved_col("a")), Some(resolved_col("b")))]
    fn join_predicate_pushes_down(
        #[case] join_type: JoinType,
        #[case] remaining_on: Option<ExprRef>,
        #[case] left_pred: Option<ExprRef>,
        #[case] right_pred: Option<ExprRef>,
    ) -> DaftResult<()> {
        let a_field = Field::new("a", DataType::Boolean);
        let b_field = Field::new("b", DataType::Boolean);

        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![a_field.clone()]));
        let right_scan_node = dummy_scan_node(dummy_scan_operator(vec![b_field.clone()]));

        let plan = left_scan_node
            .clone()
            .join(
                right_scan_node.clone(),
                Some(left_col(a_field).and(right_col(b_field))),
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        let left_expected = if let Some(left_pred) = left_pred {
            left_scan_node.filter(left_pred)?
        } else {
            left_scan_node
        };

        let right_expected = if let Some(right_pred) = right_pred {
            right_scan_node.filter(right_pred)?
        } else {
            right_scan_node
        };

        let expected = left_expected
            .join(
                right_expected,
                remaining_on,
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }
}
