use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::{count_mode::CountMode, prelude::Schema};
use daft_dsl::{AggExpr, Expr, ExprRef};

use crate::{
    logical_plan::Aggregate, ops::Source as LogicalSource, optimization::rules::OptimizerRule,
    source_info::SourceInfo, LogicalPlan,
};

/// Optimization rules for pushing Aggregation further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownAggregation {}

impl PushDownAggregation {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownAggregation {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| {
            if let LogicalPlan::Aggregate(Aggregate {
                input,
                aggregations,
                groupby,
                ..
            }) = node.as_ref()
            {
                if groupby.is_empty()
                    && aggregations.len() == 1
                    && let Some(count_mode) = is_count_expr(&aggregations[0])
                {
                    // Only handle global aggregation with no GROUP BY and a single aggregation expression
                    match input.as_ref() {
                        LogicalPlan::Source(source) => {
                            match source.source_info.as_ref() {
                                // Determine if aggregation can be pushed down based on data source type
                                SourceInfo::InMemory(_) | SourceInfo::PlaceHolder(_) => {
                                    Ok(Transformed::no(node.clone()))
                                }
                                SourceInfo::Physical(external_info) => {
                                    let scan_op = external_info.scan_state.get_scan_op().0.clone();

                                    // Check if scan operation supports aggregation pushdown and specific count mode
                                    if scan_op.supports_count_pushdown()
                                        && is_count_mode_supported(count_mode)
                                        && external_info.pushdowns.filters.is_none()
                                    {
                                        // Create new pushdown info with count aggregation
                                        let new_pushdowns = external_info
                                            .pushdowns
                                            .with_aggregation(Some(aggregations[0].clone()));

                                        let field = aggregations[0].to_field(&input.schema())?;
                                        let new_schema = Arc::new(Schema::new(vec![field]));

                                        let new_external_info =
                                            external_info.with_pushdowns(new_pushdowns);
                                        let new_source = LogicalPlan::Source(LogicalSource::new(
                                            new_schema,
                                            SourceInfo::Physical(new_external_info).into(),
                                        ))
                                        .into();
                                        Ok(Transformed::yes(new_source))
                                    } else {
                                        Ok(Transformed::no(node.clone()))
                                    }
                                }
                            }
                        }
                        // Input is not a Source node, cannot push down aggregation
                        _ => Ok(Transformed::no(node.clone())),
                    }
                } else {
                    Ok(Transformed::no(node))
                }
            } else {
                Ok(Transformed::no(node))
            }
        })
    }
}

// Check if expression is count aggregation
fn is_count_expr(expr: &ExprRef) -> Option<&CountMode> {
    match expr.as_ref() {
        Expr::Agg(AggExpr::Count(_, count_mode)) => Some(count_mode),
        _ => None,
    }
}

// Check if the count mode is supported for pushdown
// Currently only CountMode::All is fully supported
fn is_count_mode_supported(count_mode: &CountMode) -> bool {
    matches!(count_mode, CountMode::All)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_scan_info::Pushdowns;
    use daft_core::prelude::*;
    use daft_dsl::{lit, resolved_col, unresolved_col, AggExpr, Expr};

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownAggregation,
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{
            dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator_for_aggregation,
        },
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
                vec![Box::new(PushDownAggregation::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    #[test]
    fn agg_count_all() -> DaftResult<()> {
        let scan_op =
            dummy_scan_operator_for_aggregation(vec![Field::new("a", DataType::UInt64)], true);

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(vec![unresolved_col("a").count(CountMode::All)], vec![])?
            .build();

        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_aggregation(Some(Arc::new(Expr::Agg(AggExpr::Count(
                resolved_col("a"),
                CountMode::All,
            ))))),
        )
        .build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_count_valid() -> DaftResult<()> {
        let scan_op = dummy_scan_operator_for_aggregation(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
            ],
            true,
        );

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(vec![unresolved_col("a").count(CountMode::Valid)], vec![])?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_count_nul() -> DaftResult<()> {
        let scan_op = dummy_scan_operator_for_aggregation(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
            ],
            true,
        );

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(vec![unresolved_col("a").count(CountMode::Null)], vec![])?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_count_add_group() -> DaftResult<()> {
        let scan_op = dummy_scan_operator_for_aggregation(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
            ],
            true,
        );

        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(
                vec![unresolved_col("a").count(CountMode::Null)],
                vec![unresolved_col("b")],
            )?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_count_add_filter() -> DaftResult<()> {
        let scan_op = dummy_scan_operator_for_aggregation(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
            ],
            true,
        );

        let plan = dummy_scan_node(scan_op.clone())
            .filter(resolved_col("a").lt(lit(2)))?
            .aggregate(vec![unresolved_col("*").count(CountMode::Null)], vec![])?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_count_add_select() -> DaftResult<()> {
        let scan_op = dummy_scan_operator_for_aggregation(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Int64),
            ],
            true,
        );

        let plan = dummy_scan_node(scan_op.clone())
            .select(vec![resolved_col("a")])?
            .aggregate(vec![unresolved_col("a").count(CountMode::Null)], vec![])?
            .build();

        let expected = plan.clone();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
