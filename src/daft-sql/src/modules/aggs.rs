use std::sync::Arc;

use daft_dsl::{AggExpr, Expr, ExprRef, LiteralValue};
use sqlparser::ast::FunctionArg;

use crate::{
    ensure,
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    planner::SQLPlanner,
    unsupported_sql_err,
};

use super::SQLModule;

pub struct SQLModuleAggs;

impl SQLModule for SQLModuleAggs {
    fn register(parent: &mut SQLFunctions) {
        use AggExpr::*;
        // HACK TO USE AggExpr as an enum rather than a
        let nil = Arc::new(Expr::Literal(LiteralValue::Null));
        parent.add_fn(
            "count",
            Count(nil.clone(), daft_core::count_mode::CountMode::Valid),
        );
        parent.add_fn("sum", Sum(nil.clone()));
        parent.add_fn("avg", Mean(nil.clone()));
        parent.add_fn("mean", Mean(nil.clone()));
        parent.add_fn("min", Min(nil.clone()));
        parent.add_fn("max", Max(nil.clone()));
    }
}

impl SQLFunction for AggExpr {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        let inputs = self.args_to_expr_unnamed(inputs, planner)?;
        to_expr(self, inputs.as_slice())
    }
}

pub(crate) fn to_expr(expr: &AggExpr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    match expr {
        AggExpr::Count(_, _) => {
            // SQL default COUNT ignores nulls.
            ensure!(args.len() == 1, "count takes exactly one argument");
            Ok(args[0]
                .clone()
                .count(daft_core::count_mode::CountMode::Valid))
        }
        AggExpr::Sum(_) => {
            ensure!(args.len() == 1, "sum takes exactly one argument");
            Ok(args[0].clone().sum())
        }
        AggExpr::ApproxCountDistinct(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::ApproxPercentile(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::ApproxSketch(_, _) => unsupported_sql_err!("approx_sketch"),
        AggExpr::MergeSketch(_, _) => unsupported_sql_err!("merge_sketch"),
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
