use daft_dsl::{AggExpr, ExprRef};

use crate::{ensure, error::SQLPlannerResult, functions::SQLFunctions, unsupported_sql_err};

use super::SQLModule;

pub struct SQLModuleAggs;

impl SQLModule for SQLModuleAggs {
    fn register(_parent: &mut SQLFunctions) {}
}

pub(crate) fn to_expr(expr: &AggExpr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    match expr {
        AggExpr::Count(_, _) => {
            // SQL default COUNT ignores nulls.
            ensure!(args.len() == 1, "count takes exactly one argument");
            Ok(args[0].clone().count(daft_core::CountMode::Valid))
        }
        AggExpr::Sum(_) => {
            ensure!(args.len() == 1, "sum takes exactly one argument");
            Ok(args[0].clone().sum())
        }
        AggExpr::ApproxSketch(_) => unsupported_sql_err!("approx_sketch"),
        AggExpr::ApproxPercentile(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::MergeSketch(_) => unsupported_sql_err!("merge_sketch"),
        AggExpr::Mean(_) => {
            ensure!(args.len() == 1, "mean takes exactly one argument");
            Ok(args[0].clone().mean())
        }
        AggExpr::Min(_) => {
            ensure!(args.len() == 1, "min takes exactly one argument");
            Ok(args[0].clone().min())
        }
        AggExpr::Max(_) => {
            ensure!(args.len() == 1, "max takes exactly one argument");
            Ok(args[0].clone().max())
        }
        AggExpr::AnyValue(_, _) => unsupported_sql_err!("any_value"),
        AggExpr::List(_) => unsupported_sql_err!("list"),
        AggExpr::Concat(_) => unsupported_sql_err!("concat"),
        AggExpr::MapGroups { .. } => unsupported_sql_err!("map_groups"),
    }
}
