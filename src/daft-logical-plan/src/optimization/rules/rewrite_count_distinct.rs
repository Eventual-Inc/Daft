use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::prelude::CountMode;
use daft_dsl::{resolved_col, AggExpr, Expr};

use super::OptimizerRule;
use crate::{
    ops::{Aggregate, Distinct, Project},
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
                        resolved_col(cd.semantic_id(aggregate.input.schema().as_ref()).id),
                        CountMode::Valid,
                    )))))
                } else {
                    Ok(Transformed::no(e))
                }
            })?;

            let Some(sub_expr) = sub_expr else {
                assert!(!new_agg_expr.transformed);
                return Ok(Transformed::no(node));
            };

            let sub_expr_name = sub_expr.semantic_id(aggregate.input.schema().as_ref()).id;

            // TODO: UNDO
            let input_node = if sub_expr.has_compute() {
                // We need to create a project node to make this a permanent column
                // Otherwise, distinct with only use it for the groupby and not include in the output
                // Ideally, projection pushdown will clean this up
                // TODO: Consider adding a flag to DISTINCT to include generated columns in output
                LogicalPlan::Project(Project::try_new(
                    aggregate.input.clone(),
                    vec![sub_expr.alias(sub_expr_name.clone())],
                )?)
                .arced()
            } else {
                aggregate.input.clone()
            };

            let distinct_node = LogicalPlan::Distinct(Distinct::new(
                input_node,
                Some(vec![resolved_col(sub_expr_name)]),
            ))
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
    use daft_core::prelude::CountMode;
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

    // Test that aggs without count_distinct are not modified
    #[test]
    fn unmodified_unrelated_aggs() -> DaftResult<()> {
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
                ],
                vec![],
            )?
            .build();

        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    // Test that count_distinct with other aggs are not modified
    #[test]
    fn unmodified_count_distinct_with_other_aggs() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![
                    unresolved_col("a").count_distinct(),
                    unresolved_col("b").sum(),
                ],
                vec![],
            )?
            .build();

        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    // Test that count_distinct in a groupby are not modified
    #[test]
    fn unmodified_count_distinct_in_groupby() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![unresolved_col("a").count_distinct()],
                vec![unresolved_col("b")],
            )?
            .build();

        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    // Test that count_distinct is rewritten to count(distinct)
    #[test]
    fn rewrite_count_distinct_base() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(vec![unresolved_col("a").count_distinct()], vec![])?
            .build();

        let expected = dummy_scan_node(scan_op)
            .distinct(Some(vec![unresolved_col("a")]))?
            .aggregate(vec![unresolved_col("a").count(CountMode::Valid)], vec![])?
            .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    // Test that count_distinct on a projected column is rewritten to a count(distinct)
    #[test]
    fn rewrite_count_distinct_on_projected_column() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![unresolved_col("a")
                    .add(unresolved_col("b"))
                    .count_distinct()
                    .alias("a_plus_b_count_distinct")],
                vec![],
            )?
            .build();

        let expected = dummy_scan_node(scan_op)
            .select(vec![
                unresolved_col("a")
                    .add(unresolved_col("b"))
                    .alias("(a + b)"), // Semantic id is "(a + b)"
            ])?
            .distinct(Some(vec![unresolved_col("(a + b)")]))?
            .aggregate(
                vec![unresolved_col("(a + b)")
                    .count(CountMode::Valid)
                    .alias("a_plus_b_count_distinct")],
                vec![],
            )?
            .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
