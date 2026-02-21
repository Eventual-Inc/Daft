use std::sync::Arc;

use daft_dsl::{
    ExprRef,
    functions::{BuiltinScalarFn, BuiltinScalarFnVariant, FunctionArg, FunctionArgs},
    lit, null_lit,
};
use daft_functions_temporal::truncate::Truncate;
use sqlparser::ast;

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleTemporal;

impl SQLModule for SQLModuleTemporal {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("date_trunc", SQLDateTrunc);
        parent.add_fn("truncate", SQLDateTrunc);
    }
}

pub struct SQLDateTrunc;

impl SQLFunction for SQLDateTrunc {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        // SQL: DATE_TRUNC('minute', ts)           → 2 args
        // SQL: DATE_TRUNC('minute', ts, ref)      → 3 args
        match inputs.len() {
            2 | 3 => {}
            _ => invalid_operation_err!(
                "date_trunc expects 2 or 3 arguments: date_trunc(interval, input[, relative_to])"
            ),
        }

        // arg[0]: interval string literal
        let interval_expr = planner.plan_function_arg(&inputs[0])?.into_inner();
        let interval_str = interval_expr
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "date_trunc first argument must be a string literal (e.g. 'minute')",
                )
            })?;

        // Normalize: bare unit names like "minute" become "1 minute"
        let normalized = if interval_str
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
        {
            interval_str.to_string()
        } else {
            format!("1 {interval_str}")
        };

        // arg[1]: input expression (the timestamp column)
        let input_expr = planner.plan_function_arg(&inputs[1])?.into_inner();

        // arg[2]: optional relative_to expression (default: null literal)
        let relative_to_expr = if inputs.len() == 3 {
            planner.plan_function_arg(&inputs[2])?.into_inner()
        } else {
            null_lit()
        };

        // Build FunctionArgs matching the Truncate UDF's Args<T>:
        //   input: T              (unnamed)
        //   relative_to: Option<T> (unnamed, optional)
        //   interval: String       (named)
        let mut args = vec![
            FunctionArg::unnamed(input_expr),
            FunctionArg::unnamed(relative_to_expr),
        ];
        args.push(FunctionArg::named("interval".to_string(), lit(normalized)));

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(Truncate)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Truncates a timestamp to the specified time unit (e.g. 'minute', 'hour', 'day')."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["interval", "input", "relative_to"]
    }
}
