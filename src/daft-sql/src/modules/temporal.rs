use std::sync::Arc;

use daft_dsl::{
    ExprRef,
    functions::{BuiltinScalarFn, BuiltinScalarFnVariant, FunctionArg, FunctionArgs},
    lit, null_lit,
};
use daft_functions::temporal::{
    current::{CurrentDate, CurrentTimestamp, CurrentTimezone},
    date_arithmetic::{DateAdd, DateDiff, DateSub},
    date_construction::{MakeDate, MakeTimestamp, MakeTimestampLtz},
    date_navigation::{LastDay, NextDay},
    epoch_conversions::{
        DateFromUnixDate, FromUnixtime, TimestampMicros, TimestampMillis, TimestampSeconds,
    },
    truncate::Truncate,
};
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
        parent.add_fn("current_date", SQLCurrentDate);
        parent.add_fn("current_timestamp", SQLCurrentTimestamp);
        parent.add_fn("current_timezone", SQLCurrentTimezone);
        parent.add_fn("date_add", SQLDateAdd);
        parent.add_fn("date_sub", SQLDateSub);
        parent.add_fn("date_diff", SQLDateDiff);
        parent.add_fn("date_from_unix_date", SQLDateFromUnixDate);
        parent.add_fn("timestamp_seconds", SQLTimestampSeconds);
        parent.add_fn("timestamp_millis", SQLTimestampMillis);
        parent.add_fn("timestamp_micros", SQLTimestampMicros);
        parent.add_fn("from_unixtime", SQLFromUnixtime);
        parent.add_fn("make_date", SQLMakeDate);
        parent.add_fn("make_timestamp", SQLMakeTimestamp);
        parent.add_fn("make_timestamp_ltz", SQLMakeTimestampLtz);
        parent.add_fn("last_day", SQLLastDay);
        parent.add_fn("next_day", SQLNextDay);
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
        "Truncates a timestamp to the specified interval (e.g. 'minute', 'hour', 'day')."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["interval", "input", "relative_to"]
    }
}

// --- Zero-arg temporal SQL functions ---

pub struct SQLCurrentDate;

impl SQLFunction for SQLCurrentDate {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        _planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if !inputs.is_empty() {
            invalid_operation_err!("current_date expects 0 arguments, got {}", inputs.len());
        }
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(CurrentDate)),
            inputs: FunctionArgs::new_unchecked(vec![]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the current date (UTC).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLCurrentTimestamp;

impl SQLFunction for SQLCurrentTimestamp {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        _planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if !inputs.is_empty() {
            invalid_operation_err!(
                "current_timestamp expects 0 arguments, got {}",
                inputs.len()
            );
        }
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(CurrentTimestamp)),
            inputs: FunctionArgs::new_unchecked(vec![]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the current timestamp (UTC) with microsecond precision.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLCurrentTimezone;

impl SQLFunction for SQLCurrentTimezone {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        _planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if !inputs.is_empty() {
            invalid_operation_err!("current_timezone expects 0 arguments, got {}", inputs.len());
        }
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(CurrentTimezone)),
            inputs: FunctionArgs::new_unchecked(vec![]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the current timezone (always 'UTC' in Daft).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

// --- Two-arg temporal SQL functions ---

pub struct SQLDateAdd;

impl SQLFunction for SQLDateAdd {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("date_add expects 2 arguments, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let days = planner.plan_function_arg(&inputs[1])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(DateAdd)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(input),
                FunctionArg::unnamed(days),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Adds a number of days to a date.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "days"]
    }
}

pub struct SQLDateSub;

impl SQLFunction for SQLDateSub {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("date_sub expects 2 arguments, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let days = planner.plan_function_arg(&inputs[1])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(DateSub)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(input),
                FunctionArg::unnamed(days),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Subtracts a number of days from a date.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "days"]
    }
}

pub struct SQLDateDiff;

impl SQLFunction for SQLDateDiff {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("date_diff expects 2 arguments, got {}", inputs.len());
        }
        let end_date = planner.plan_function_arg(&inputs[0])?.into_inner();
        let start_date = planner.plan_function_arg(&inputs[1])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(DateDiff)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(end_date),
                FunctionArg::unnamed(start_date),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the number of days between two dates.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["end_date", "start_date"]
    }
}

// --- One-arg epoch conversion SQL functions ---

pub struct SQLDateFromUnixDate;

impl SQLFunction for SQLDateFromUnixDate {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!(
                "date_from_unix_date expects 1 argument, got {}",
                inputs.len()
            );
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(DateFromUnixDate)),
            inputs: FunctionArgs::new_unchecked(vec![FunctionArg::unnamed(input)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Converts days since epoch to a date.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["days"]
    }
}

pub struct SQLTimestampSeconds;

impl SQLFunction for SQLTimestampSeconds {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("timestamp_seconds expects 1 argument, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(TimestampSeconds)),
            inputs: FunctionArgs::new_unchecked(vec![FunctionArg::unnamed(input)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Creates a timestamp from seconds since epoch.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["seconds"]
    }
}

pub struct SQLTimestampMillis;

impl SQLFunction for SQLTimestampMillis {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("timestamp_millis expects 1 argument, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(TimestampMillis)),
            inputs: FunctionArgs::new_unchecked(vec![FunctionArg::unnamed(input)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Creates a timestamp from milliseconds since epoch.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["millis"]
    }
}

pub struct SQLTimestampMicros;

impl SQLFunction for SQLTimestampMicros {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("timestamp_micros expects 1 argument, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(TimestampMicros)),
            inputs: FunctionArgs::new_unchecked(vec![FunctionArg::unnamed(input)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Creates a timestamp from microseconds since epoch.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["micros"]
    }
}

pub struct SQLFromUnixtime;

impl SQLFunction for SQLFromUnixtime {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.is_empty() || inputs.len() > 2 {
            invalid_operation_err!(
                "from_unixtime expects 1 or 2 arguments, got {}",
                inputs.len()
            );
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let mut args = vec![FunctionArg::unnamed(input)];
        if inputs.len() == 2 {
            let format = planner.plan_function_arg(&inputs[1])?.into_inner();
            args.push(FunctionArg::unnamed(format));
        }
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(FromUnixtime)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Converts unix timestamp (seconds) to a formatted string.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["seconds", "format"]
    }
}

// --- Date construction SQL functions ---

pub struct SQLMakeDate;

impl SQLFunction for SQLMakeDate {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 3 {
            invalid_operation_err!(
                "make_date expects 3 arguments (year, month, day), got {}",
                inputs.len()
            );
        }
        let year = planner.plan_function_arg(&inputs[0])?.into_inner();
        let month = planner.plan_function_arg(&inputs[1])?.into_inner();
        let day = planner.plan_function_arg(&inputs[2])?.into_inner();

        let args = vec![
            FunctionArg::unnamed(year),
            FunctionArg::unnamed(month),
            FunctionArg::unnamed(day),
        ];

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(MakeDate)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Creates a date from year, month, and day components.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["year", "month", "day"]
    }
}

pub struct SQLMakeTimestamp;

impl SQLFunction for SQLMakeTimestamp {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs.len() {
            6 | 7 => {}
            _ => invalid_operation_err!(
                "make_timestamp expects 6 or 7 arguments (year, month, day, hour, minute, second[, timezone]), got {}",
                inputs.len()
            ),
        }
        let year = planner.plan_function_arg(&inputs[0])?.into_inner();
        let month = planner.plan_function_arg(&inputs[1])?.into_inner();
        let day = planner.plan_function_arg(&inputs[2])?.into_inner();
        let hour = planner.plan_function_arg(&inputs[3])?.into_inner();
        let minute = planner.plan_function_arg(&inputs[4])?.into_inner();
        let second = planner.plan_function_arg(&inputs[5])?.into_inner();

        let mut args = vec![
            FunctionArg::unnamed(year),
            FunctionArg::unnamed(month),
            FunctionArg::unnamed(day),
            FunctionArg::unnamed(hour),
            FunctionArg::unnamed(minute),
            FunctionArg::unnamed(second),
        ];

        if inputs.len() == 7 {
            let tz_expr = planner.plan_function_arg(&inputs[6])?.into_inner();
            let tz_str = tz_expr
                .as_literal()
                .and_then(|l| l.as_str())
                .ok_or_else(|| {
                    crate::error::PlannerError::invalid_operation(
                        "make_timestamp timezone argument must be a string literal",
                    )
                })?;
            args.push(FunctionArg::named(
                "timezone".to_string(),
                lit(tz_str.to_string()),
            ));
        }

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(MakeTimestamp)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Creates a timestamp from year, month, day, hour, minute, second components.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "year", "month", "day", "hour", "minute", "second", "timezone",
        ]
    }
}

pub struct SQLMakeTimestampLtz;

impl SQLFunction for SQLMakeTimestampLtz {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs.len() {
            6 | 7 => {}
            _ => invalid_operation_err!(
                "make_timestamp_ltz expects 6 or 7 arguments (year, month, day, hour, minute, second[, timezone]), got {}",
                inputs.len()
            ),
        }
        let year = planner.plan_function_arg(&inputs[0])?.into_inner();
        let month = planner.plan_function_arg(&inputs[1])?.into_inner();
        let day = planner.plan_function_arg(&inputs[2])?.into_inner();
        let hour = planner.plan_function_arg(&inputs[3])?.into_inner();
        let minute = planner.plan_function_arg(&inputs[4])?.into_inner();
        let second = planner.plan_function_arg(&inputs[5])?.into_inner();

        let mut args = vec![
            FunctionArg::unnamed(year),
            FunctionArg::unnamed(month),
            FunctionArg::unnamed(day),
            FunctionArg::unnamed(hour),
            FunctionArg::unnamed(minute),
            FunctionArg::unnamed(second),
        ];

        if inputs.len() == 7 {
            let tz_expr = planner.plan_function_arg(&inputs[6])?.into_inner();
            let tz_str = tz_expr
                .as_literal()
                .and_then(|l| l.as_str())
                .ok_or_else(|| {
                    crate::error::PlannerError::invalid_operation(
                        "make_timestamp_ltz timezone argument must be a string literal",
                    )
                })?;
            args.push(FunctionArg::named(
                "timezone".to_string(),
                lit(tz_str.to_string()),
            ));
        }

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(MakeTimestampLtz)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Creates a UTC timestamp from components, optionally interpreting them in a source timezone."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "year", "month", "day", "hour", "minute", "second", "timezone",
        ]
    }
}

// --- Date navigation SQL functions ---

pub struct SQLLastDay;

impl SQLFunction for SQLLastDay {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("last_day expects 1 argument, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();

        let args = vec![FunctionArg::unnamed(input)];

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(LastDay)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the last day of the month for the given date.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["date"]
    }
}

pub struct SQLNextDay;

impl SQLFunction for SQLNextDay {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!(
                "next_day expects 2 arguments (date, day_of_week), got {}",
                inputs.len()
            );
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let dow_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
        let dow_str = dow_expr
            .as_literal()
            .and_then(|l| l.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "next_day day_of_week argument must be a string literal (e.g. 'Monday')",
                )
            })?;

        let args = vec![
            FunctionArg::unnamed(input),
            FunctionArg::named("day_of_week".to_string(), lit(dow_str.to_string())),
        ];

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(NextDay)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the next occurrence of the specified day of the week after the given date."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["date", "day_of_week"]
    }
}
