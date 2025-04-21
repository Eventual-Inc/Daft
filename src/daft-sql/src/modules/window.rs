use daft_dsl::{Expr, ExprRef, WindowExpr};
use sqlparser::ast::FunctionArg;

use crate::{
    error::SQLPlannerResult, functions::SQLFunction, modules::SQLModule, planner::SQLPlanner,
};

pub struct SQLRowNumber;

impl SQLFunction for SQLRowNumber {
    fn to_expr(&self, inputs: &[FunctionArg], _planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(
            inputs.is_empty(),
            "ROW_NUMBER() does not take any arguments"
        );
        Ok(Expr::WindowFunction(WindowExpr::RowNumber).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}() OVER() - Returns the sequential row number starting at 1 within a partition"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLModuleWindow;

impl SQLModule for SQLModuleWindow {
    fn register(registry: &mut crate::functions::SQLFunctions) {
        registry.add_fn("row_number", SQLRowNumber {});
    }
}
