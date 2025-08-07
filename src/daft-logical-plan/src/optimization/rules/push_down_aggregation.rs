use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::Aggregation;
use common_treenode::Transformed;
use daft_core::count_mode::CountMode;
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_schema::{dtype::DataType, field::Field, schema::Schema};

use crate::{
    logical_plan::{Aggregate, LogicalPlan},
    ops::Source as LogicalSource,
    optimization::rules::OptimizerRule,
    source_info::SourceInfo,
};

/// Optimization rules for pushing Filters further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownAggregation;

impl OptimizerRule for PushDownAggregation {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggregations,
                groupby,
                ..
            }) if groupby.is_empty() => {
                // ONLY handle global aggregations (no GROUP BY)

                // CHECK if there is only one count aggregation
                if aggregations.len() == 1 {
                    if let Some(count_expr) = is_count_expr(&aggregations[0]) {
                        // REPLACE match with if let to match Source node
                        if let LogicalPlan::Source(source) = input.as_ref() {
                            let source_info = source.source_info.as_ref();
                            if let SourceInfo::Physical(external_info) = source_info {
                                // CHECK if scan operator supports count pushdown
                                let scan_op = external_info.scan_state.get_scan_op().clone().0;
                                if scan_op.can_absorb_aggregation()
                                    && scan_op.supports_count_pushdown()
                                {
                                    // CREATE new pushdowns with count aggregation
                                    let count_mode = extract_count_mode(count_expr);
                                    let new_pushdowns = external_info
                                        .pushdowns
                                        .with_aggregation(Some(Aggregation::Count(count_mode)));
                                    let new_external_info =
                                        external_info.with_pushdowns(new_pushdowns);

                                    // CREATE new data source node with count aggregation pushdown
                                    let new_source = LogicalPlan::Source(LogicalSource::new(
                                        // OUTPUT schema should be single column of UInt64 type
                                        Schema::new(vec![Field::new("count", DataType::UInt64)])
                                            .into(),
                                        SourceInfo::Physical(new_external_info).into(),
                                    ))
                                    .into();

                                    return Ok(Transformed::yes(new_source));
                                }
                            }
                        }
                    }
                }

                // IF no count aggregation pushdown, return original plan
                Ok(Transformed::no(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

// CHECK if expression is count aggregation
fn is_count_expr(expr: &ExprRef) -> Option<&CountMode> {
    match expr.as_ref() {
        Expr::Agg(agg) => match agg {
            AggExpr::Count(_, count_mode) => Some(count_mode),
            AggExpr::CountDistinct(_) => Some(&CountMode::Valid),
            _ => None,
        },
        _ => None,
    }
}
// EXTRACT count mode from CountMode enum
fn extract_count_mode(count_mode: &CountMode) -> CountMode {
    *count_mode
}
