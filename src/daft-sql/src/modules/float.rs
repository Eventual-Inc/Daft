use daft_dsl::ExprRef;
use daft_functions::float;
use sqlparser::ast::FunctionArg;

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleFloat;

impl SQLModule for SQLModuleFloat {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("fill_nan", SQLFillNan {});
        parent.add_fn("is_inf", SQLIsInf {});
        parent.add_fn("is_nan", SQLIsNan {});
        parent.add_fn("not_nan", SQLNotNan {});
    }
}

pub struct SQLFillNan {}

impl SQLFunction for SQLFillNan {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, fill_value] => {
                let input = planner.plan_function_arg(input)?;
                let fill_value = planner.plan_function_arg(fill_value)?;
                Ok(float::fill_nan(input, fill_value))
            }
            _ => unsupported_sql_err!("Invalid arguments for 'fill_nan': '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::FILL_NAN_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "fill_value"]
    }
}

pub struct SQLIsInf {}

impl SQLFunction for SQLIsInf {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => planner.plan_function_arg(input).map(float::is_inf),
            _ => unsupported_sql_err!("Invalid arguments for 'is_inf': '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::IS_INF_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLIsNan {}

impl SQLFunction for SQLIsNan {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => planner.plan_function_arg(input).map(float::is_nan),
            _ => unsupported_sql_err!("Invalid arguments for 'is_nan': '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::IS_NAN_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLNotNan {}

impl SQLFunction for SQLNotNan {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => planner.plan_function_arg(input).map(float::not_nan),
            _ => unsupported_sql_err!("Invalid arguments for 'not_nan': '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::NOT_NAN_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

mod static_docs {
    pub(crate) const FILL_NAN_DOCSTRING: &str =
        "Replaces NaN values in the input expression with a specified fill value.";

    pub(crate) const IS_INF_DOCSTRING: &str =
        "Checks if the input expression is infinite (positive or negative infinity).";

    pub(crate) const IS_NAN_DOCSTRING: &str =
        "Checks if the input expression is NaN (Not a Number).";

    pub(crate) const NOT_NAN_DOCSTRING: &str =
        "Checks if the input expression is not NaN (Not a Number).";
}
