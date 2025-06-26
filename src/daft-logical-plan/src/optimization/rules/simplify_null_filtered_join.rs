use std::collections::HashSet;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_algebra::boolean::predicate_removes_nulls;
use daft_core::join::JoinType;
use daft_dsl::{resolved_col, ExprRef};

use super::OptimizerRule;
use crate::{
    ops::{Filter, Join},
    LogicalPlan, LogicalPlanRef,
};

/// Left, right, and outer joins can be simplified if there is a filter on their null-producing side.
///
/// This is especially useful for enabling other optimizations that rely on the join type, such as join reordering.
///
/// Examples:
/// 1. select * from A left join B where B.x > 0 -> select * from A inner join B where B.x > 0
/// 2. select * from A full outer join B where B.x > 0 -> select * from A right join B where B.x > 0
/// 3. select * from A full outer join B where A.x > 0 and B.y > 0 -> select * from A inner join B where A.x > 0 and B.y > 0
#[derive(Debug)]
pub struct SimplifyNullFilteredJoin {}

impl SimplifyNullFilteredJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SimplifyNullFilteredJoin {
    fn try_optimize(&self, plan: LogicalPlanRef) -> DaftResult<Transformed<LogicalPlanRef>> {
        plan.transform(|node| {
            if let LogicalPlan::Filter(Filter {
                input, predicate, ..
            }) = node.as_ref()
            {
                match input.as_ref() {
                    LogicalPlan::Join(Join {
                        left,
                        right,
                        on,
                        join_type,
                        join_strategy,
                        ..
                    }) if matches!(
                        join_type,
                        JoinType::Left | JoinType::Right | JoinType::Outer
                    ) =>
                    {
                        let left_preserved = *join_type == JoinType::Left || {
                            let left_cols: HashSet<ExprRef> =
                                HashSet::from_iter(left.schema().field_names().map(resolved_col));

                            predicate_removes_nulls(predicate.clone(), &input.schema(), &left_cols)?
                        };

                        let right_preserved = *join_type == JoinType::Right || {
                            let right_cols: HashSet<ExprRef> =
                                HashSet::from_iter(right.schema().field_names().map(resolved_col));

                            predicate_removes_nulls(
                                predicate.clone(),
                                &input.schema(),
                                &right_cols,
                            )?
                        };

                        let simplified_join_type = match (left_preserved, right_preserved) {
                            (true, true) => JoinType::Inner,
                            (true, false) => JoinType::Left,
                            (false, true) => JoinType::Right,
                            (false, false) => JoinType::Outer,
                        };

                        if *join_type != simplified_join_type {
                            let new_join = Join::try_new(
                                left.clone(),
                                right.clone(),
                                on.clone(),
                                simplified_join_type,
                                *join_strategy,
                            )?
                            .into();
                            return Ok(Transformed::yes(
                                node.with_new_children(&[new_join]).into(),
                            ));
                        }
                    }
                    _ => {}
                }
            }

            Ok(Transformed::no(node))
        })
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;
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
                vec![Box::new(SimplifyNullFilteredJoin::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[rstest]
    #[case(JoinType::Left, resolved_col("b"), JoinType::Inner)]
    #[case(JoinType::Left, resolved_col("a"), JoinType::Left)]
    #[case(JoinType::Right, resolved_col("a"), JoinType::Inner)]
    #[case(JoinType::Right, resolved_col("b"), JoinType::Right)]
    #[case(JoinType::Outer, resolved_col("a").eq(resolved_col("b")), JoinType::Inner)]
    #[case(JoinType::Outer, resolved_col("a"), JoinType::Left)]
    #[case(JoinType::Outer, resolved_col("b"), JoinType::Right)]
    #[case(JoinType::Outer, resolved_col("a").eq_null_safe(resolved_col("b")), JoinType::Outer)]
    fn join_type_simplifies(
        #[case] join_type: JoinType,
        #[case] filter_predicate: ExprRef,
        #[case] expected_join_type: JoinType,
    ) -> DaftResult<()> {
        let left_scan_node = dummy_scan_node(dummy_scan_operator(vec![Field::new(
            "a",
            DataType::Boolean,
        )]));
        let right_scan_node = dummy_scan_node(dummy_scan_operator(vec![Field::new(
            "b",
            DataType::Boolean,
        )]));

        let plan = left_scan_node
            .join(
                right_scan_node.clone(),
                None,
                vec![],
                join_type,
                None,
                Default::default(),
            )?
            .filter(filter_predicate.clone())?
            .build();

        let expected = left_scan_node
            .join(
                right_scan_node,
                None,
                vec![],
                expected_join_type,
                None,
                Default::default(),
            )?
            .filter(filter_predicate.clone())?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }
}
