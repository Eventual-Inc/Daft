use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::prelude::CountMode;
use daft_dsl::{resolved_col, AggExpr, Expr};

use super::OptimizerRule;
use crate::{
    ops::{Aggregate, Distinct},
    LogicalPlan,
};

/// Basic rewrite rule for converting `count_distinct(col)` to `count(distinct(col))`. Should only
/// apply to ungrouped aggregations.
#[derive(Default, Debug)]
pub struct RewriteCountDistinct {}

impl RewriteCountDistinct {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RewriteCountDistinct {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| {
            let LogicalPlan::Aggregate(aggregate) = node.as_ref() else {
                return Ok(Transformed::no(node));
            };

            if !aggregate.groupby.is_empty() || aggregate.aggregations.len() != 1 {
                return Ok(Transformed::no(node));
            }

            let agg_expr = aggregate.aggregations.first().unwrap();

            let mut sub_expr = None;
            let new_agg_expr = agg_expr.clone().transform_down(|e| {
                if let Expr::Agg(AggExpr::CountDistinct(cd)) = e.as_ref() {
                    sub_expr = Some(cd.clone());
                    // TODO: Use semantic id or generate random one?
                    Ok(Transformed::yes(Arc::new(Expr::Agg(AggExpr::Count(
                        resolved_col(e.semantic_id(aggregate.input.schema().as_ref()).id),
                        CountMode::All,
                    )))))
                } else {
                    Ok(Transformed::no(e))
                }
            })?;

            let Some(sub_expr) = sub_expr else {
                assert!(!new_agg_expr.transformed);
                return Ok(Transformed::no(node));
            };
            let distinct_node =
                LogicalPlan::Distinct(Distinct::new(aggregate.input.clone(), Some(vec![sub_expr])))
                    .arced();

            let count_node = LogicalPlan::Aggregate(Aggregate::try_new(
                distinct_node,
                vec![new_agg_expr.data],
                vec![],
            )?)
            .arced();

            Ok(Transformed::yes(count_node))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_dsl::unresolved_col;
    use daft_schema::{dtype::DataType, field::Field};

    use super::RewriteCountDistinct;
    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
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
                vec![Box::new(RewriteCountDistinct::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn lift_exprs_global_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .sum()
                        .add(unresolved_col("b").sum())
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![],
            )?
            .build();

        let schema = dummy_scan_node(scan_op.clone()).schema();

        let a_sum_id = unresolved_col("a").sum().semantic_id(schema.as_ref()).id;
        let b_sum_id = unresolved_col("b").sum().semantic_id(schema.as_ref()).id;
        let b_mean_id = unresolved_col("b").mean().semantic_id(schema.as_ref()).id;

        let expected = dummy_scan_node(scan_op)
            .aggregate(
                vec![
                    unresolved_col("a").sum().alias(a_sum_id.clone()),
                    unresolved_col("b").sum().alias(b_sum_id.clone()),
                    unresolved_col("b").mean().alias(b_mean_id.clone()),
                ],
                vec![],
            )?
            .select(vec![
                unresolved_col(a_sum_id.clone()).alias("a"),
                unresolved_col(a_sum_id)
                    .add(unresolved_col(b_sum_id))
                    .alias("a_plus_b"),
                unresolved_col(b_mean_id.clone()).alias("b"),
                unresolved_col(b_mean_id).alias("c"),
            ])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn lift_exprs_groupby_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("groupby_key", DataType::Utf8),
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .sum()
                        .add(unresolved_col("b").sum())
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![unresolved_col("groupby_key")],
            )?
            .build();

        let schema = dummy_scan_node(scan_op.clone()).schema();

        let a_sum_id = unresolved_col("a").sum().semantic_id(schema.as_ref()).id;
        let b_sum_id = unresolved_col("b").sum().semantic_id(schema.as_ref()).id;
        let b_mean_id = unresolved_col("b").mean().semantic_id(schema.as_ref()).id;

        let expected = dummy_scan_node(scan_op)
            .aggregate(
                vec![
                    unresolved_col("a").sum().alias(a_sum_id.clone()),
                    unresolved_col("b").sum().alias(b_sum_id.clone()),
                    unresolved_col("b").mean().alias(b_mean_id.clone()),
                ],
                vec![unresolved_col("groupby_key")],
            )?
            .select(vec![
                unresolved_col("groupby_key"),
                unresolved_col(a_sum_id.clone()).alias("a"),
                unresolved_col(a_sum_id)
                    .add(unresolved_col(b_sum_id))
                    .alias("a_plus_b"),
                unresolved_col(b_mean_id.clone()).alias("b"),
                unresolved_col(b_mean_id).alias("c"),
            ])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn do_not_lift_exprs_global_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .add(unresolved_col("b"))
                        .sum()
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![],
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn do_not_lift_exprs_groupby_agg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("groupby_key", DataType::Utf8),
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").sum(),
                    unresolved_col("a")
                        .add(unresolved_col("b"))
                        .sum()
                        .alias("a_plus_b"),
                    unresolved_col("b").mean(),
                    unresolved_col("b").mean().alias("c"),
                ],
                vec![unresolved_col("groupby_key")],
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
