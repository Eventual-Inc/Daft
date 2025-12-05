use common_error::{DaftError, DaftResult};

use crate::{AggExpr, ApproxPercentileParams, Expr, ExprRef};

pub fn extract_agg_expr(expr: &ExprRef) -> DaftResult<AggExpr> {
    match expr.as_ref() {
        Expr::Agg(agg_expr) => Ok(agg_expr.clone()),
        Expr::Alias(e, name) => extract_agg_expr(e).map(|agg_expr| {
            // reorder expressions so that alias goes before agg
            match agg_expr {
                AggExpr::Count(e, count_mode) => {
                    AggExpr::Count(Expr::Alias(e, name.clone()).into(), count_mode)
                }
                AggExpr::CountDistinct(e) => {
                    AggExpr::CountDistinct(Expr::Alias(e, name.clone()).into())
                }
                AggExpr::Sum(e) => AggExpr::Sum(Expr::Alias(e, name.clone()).into()),
                AggExpr::Product(e) => AggExpr::Product(Expr::Alias(e, name.clone()).into()),
                AggExpr::ApproxPercentile(ApproxPercentileParams {
                    child: e,
                    percentiles,
                    force_list_output,
                }) => AggExpr::ApproxPercentile(ApproxPercentileParams {
                    child: Expr::Alias(e, name.clone()).into(),
                    percentiles,
                    force_list_output,
                }),
                AggExpr::ApproxCountDistinct(e) => {
                    AggExpr::ApproxCountDistinct(Expr::Alias(e, name.clone()).into())
                }
                AggExpr::ApproxSketch(e, sketch_type) => {
                    AggExpr::ApproxSketch(Expr::Alias(e, name.clone()).into(), sketch_type)
                }
                AggExpr::MergeSketch(e, sketch_type) => {
                    AggExpr::MergeSketch(Expr::Alias(e, name.clone()).into(), sketch_type)
                }
                AggExpr::Mean(e) => AggExpr::Mean(Expr::Alias(e, name.clone()).into()),
                AggExpr::Stddev(e) => AggExpr::Stddev(Expr::Alias(e, name.clone()).into()),
                AggExpr::Min(e) => AggExpr::Min(Expr::Alias(e, name.clone()).into()),
                AggExpr::Max(e) => AggExpr::Max(Expr::Alias(e, name.clone()).into()),
                AggExpr::BoolAnd(e) => AggExpr::BoolAnd(Expr::Alias(e, name.clone()).into()),
                AggExpr::BoolOr(e) => AggExpr::BoolOr(Expr::Alias(e, name.clone()).into()),
                AggExpr::AnyValue(e, ignore_nulls) => {
                    AggExpr::AnyValue(Expr::Alias(e, name.clone()).into(), ignore_nulls)
                }
                AggExpr::List(e) => AggExpr::List(Expr::Alias(e, name.clone()).into()),
                AggExpr::Set(e) => AggExpr::Set(Expr::Alias(e, name.clone()).into()),
                AggExpr::Concat(e) => AggExpr::Concat(Expr::Alias(e, name.clone()).into()),
                AggExpr::Skew(e) => AggExpr::Skew(Expr::Alias(e, name.clone()).into()),
                AggExpr::MapGroups { func, inputs } => AggExpr::MapGroups {
                    func,
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.alias(name.clone()))
                        .collect(),
                },
            }
        }),
        _ => Err(DaftError::InternalError(format!(
            "Expected non-agg expressions in aggregation to be factored out before plan translation. Got: {:?}",
            expr
        ))),
    }
}
