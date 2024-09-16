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
}
