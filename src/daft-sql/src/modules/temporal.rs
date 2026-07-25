use std::sync::Arc;

use daft_dsl::{
    ExprRef,
    functions::{BuiltinScalarFn, BuiltinScalarFnVariant, FunctionArg, FunctionArgs, ScalarUDF},
    lit, null_lit,
};
use daft_functions_temporal::{
    Date, Day, DayOfMonth, DayOfWeek, DayOfYear, Hour, Microsecond, Millisecond, Minute, Month,
    Nanosecond, Quarter, Second, ToString, UnixDate, WeekOfYear, Year,
    current::{CurrentDate, CurrentTimestamp, CurrentTimezone},
    date_arithmetic::{AddMonths, DateAdd, DateDiff, DateSub, MonthsBetween},
    date_construction::{MakeDate, MakeTimestamp, MakeTimestampLtz},
    date_navigation::{LastDay, NextDay},
    epoch_conversions::{
        DateFromUnixDate, FromUnixtime, TimestampMicros, TimestampMillis, TimestampSeconds,
    },
    time::{ConvertTimeZone, FromUtcTimestamp, ToUtcTimestamp},
    truncate::Truncate,
    unix_timestamp::UnixTimestamp,
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
        parent.add_fn("trunc", SQLTrunc);
        parent.add_fn("dayofmonth", Arc::new(DayOfMonth) as Arc<dyn ScalarUDF>);
        parent.add_fn("dayofyear", Arc::new(DayOfYear) as Arc<dyn ScalarUDF>);
        parent.add_fn("weekofyear", Arc::new(WeekOfYear) as Arc<dyn ScalarUDF>);
        parent.add_fn("date_format", Arc::new(ToString) as Arc<dyn ScalarUDF>);
        parent.add_fn("dateadd", SQLDateAdd);
        parent.add_fn("datediff", SQLDateDiff);
        parent.add_fn("datepart", SQLDatePart);
        parent.add_fn("current_date", SQLCurrentDate);
        parent.add_fn("current_timestamp", SQLCurrentTimestamp);
        parent.add_fn("current_timezone", SQLCurrentTimezone);
        parent.add_fn("date_add", SQLDateAdd);
        parent.add_fn("date_sub", SQLDateSub);
        parent.add_fn("date_diff", SQLDateDiff);
        parent.add_fn("add_months", SQLAddMonths);
        parent.add_fn("months_between", SQLMonthsBetween);
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
        parent.add_fn("from_utc_timestamp", SQLFromUtcTimestamp);
        parent.add_fn("to_utc_timestamp", SQLToUtcTimestamp);
        parent.add_fn("convert_timezone", SQLConvertTimezone);
        parent.add_fn("unix_seconds", SQLUnixSeconds);
        parent.add_fn("unix_millis", SQLUnixMillis);
        parent.add_fn("unix_micros", SQLUnixMicros);
        parent.add_fn("unix_timestamp", SQLUnixTimestamp);
        parent.add_fn("to_unix_timestamp", SQLUnixTimestamp);
        parent.add_fn("weekday", SQLWeekday);
    }
}

fn unary_temporal_expr<UDF: ScalarUDF + 'static>(udf: UDF, input: ExprRef) -> ExprRef {
    BuiltinScalarFn {
        func: BuiltinScalarFnVariant::Sync(Arc::new(udf)),
        inputs: FunctionArgs::new_unchecked(vec![FunctionArg::unnamed(input)]),
    }
    .into()
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

pub struct SQLTrunc;

impl SQLFunction for SQLTrunc {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("trunc expects 2 arguments: trunc(input, interval)");
        }

        let input_expr = planner.plan_function_arg(&inputs[0])?.into_inner();
        let interval_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
        let interval_str = interval_expr
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "trunc second argument must be a string literal (e.g. 'month')",
                )
            })?;

        let normalized = if interval_str
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
        {
            interval_str.to_string()
        } else {
            format!("1 {interval_str}")
        };

        let args = vec![
            FunctionArg::unnamed(input_expr),
            FunctionArg::unnamed(null_lit()),
            FunctionArg::named("interval".to_string(), lit(normalized)),
        ];

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(Truncate)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Spark-style trunc alias with argument order trunc(input, interval).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "interval"]
    }
}

pub struct SQLDatePart;

impl SQLFunction for SQLDatePart {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!(
                "datepart expects 2 arguments: datepart(part, input), got {}",
                inputs.len()
            );
        }

        let part_expr = planner.plan_function_arg(&inputs[0])?.into_inner();
        let part = part_expr
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "datepart first argument must be a string literal",
                )
            })?
            .to_lowercase()
            .replace(' ', "_");
        let input = planner.plan_function_arg(&inputs[1])?.into_inner();

        match part.as_str() {
            "year" => Ok(unary_temporal_expr(Year, input)),
            "quarter" => Ok(unary_temporal_expr(Quarter, input)),
            "month" => Ok(unary_temporal_expr(Month, input)),
            "day" => Ok(unary_temporal_expr(Day, input)),
            "day_of_week" | "dayofweek" | "weekday" => Ok(unary_temporal_expr(DayOfWeek, input)),
            "day_of_month" | "dayofmonth" => Ok(unary_temporal_expr(DayOfMonth, input)),
            "day_of_year" | "dayofyear" => Ok(unary_temporal_expr(DayOfYear, input)),
            "week_of_year" | "weekofyear" | "week" => Ok(unary_temporal_expr(WeekOfYear, input)),
            "date" => Ok(unary_temporal_expr(Date, input)),
            "hour" => Ok(unary_temporal_expr(Hour, input)),
            "minute" => Ok(unary_temporal_expr(Minute, input)),
            "second" => Ok(unary_temporal_expr(Second, input)),
            "millisecond" => Ok(unary_temporal_expr(Millisecond, input)),
            "microsecond" => Ok(unary_temporal_expr(Microsecond, input)),
            "nanosecond" => Ok(unary_temporal_expr(Nanosecond, input)),
            "unix_date" => Ok(unary_temporal_expr(UnixDate, input)),
            _ => invalid_operation_err!("Unsupported datepart value: {}", part),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Alias-style extractor equivalent to extract(field from input).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["part", "input"]
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

// --- AddMonths / MonthsBetween (Spark-style month arithmetic) ---

pub struct SQLAddMonths;

impl SQLFunction for SQLAddMonths {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("add_months expects 2 arguments, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let months = planner.plan_function_arg(&inputs[1])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(AddMonths)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(input),
                FunctionArg::unnamed(months),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Adds a number of months to a date or timestamp, clamping to the last day of the target month if needed."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "months"]
    }
}

pub struct SQLMonthsBetween;

impl SQLFunction for SQLMonthsBetween {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("months_between expects 2 arguments, got {}", inputs.len());
        }
        let end_date = planner.plan_function_arg(&inputs[0])?.into_inner();
        let start_date = planner.plan_function_arg(&inputs[1])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(MonthsBetween)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(end_date),
                FunctionArg::unnamed(start_date),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the number of months between two dates or timestamps as a Float64.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["end_date", "start_date"]
    }
}

// --- Timezone conversion SQL functions ---

pub struct SQLFromUtcTimestamp;

impl SQLFunction for SQLFromUtcTimestamp {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!(
                "from_utc_timestamp expects 2 arguments, got {}",
                inputs.len()
            );
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let tz_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
        let tz_str = tz_expr
            .as_literal()
            .and_then(|l| l.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "from_utc_timestamp timezone argument must be a string literal",
                )
            })?;

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(FromUtcTimestamp)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(input),
                FunctionArg::named("timezone".to_string(), lit(tz_str.to_string())),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Interprets a UTC timestamp and returns the wall-clock time in the given timezone."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "timezone"]
    }
}

pub struct SQLToUtcTimestamp;

impl SQLFunction for SQLToUtcTimestamp {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("to_utc_timestamp expects 2 arguments, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        let tz_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
        let tz_str = tz_expr
            .as_literal()
            .and_then(|l| l.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "to_utc_timestamp timezone argument must be a string literal",
                )
            })?;

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(ToUtcTimestamp)),
            inputs: FunctionArgs::new_unchecked(vec![
                FunctionArg::unnamed(input),
                FunctionArg::named("timezone".to_string(), lit(tz_str.to_string())),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Interprets the wall-clock timestamp as being in the given timezone and returns the UTC instant."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "timezone"]
    }
}

pub struct SQLConvertTimezone;

impl SQLFunction for SQLConvertTimezone {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        // Spark argument order: convert_timezone(target_tz, source_ts).
        if inputs.len() != 2 {
            invalid_operation_err!("convert_timezone expects 2 arguments, got {}", inputs.len());
        }
        let target_expr = planner.plan_function_arg(&inputs[0])?.into_inner();
        let target_str = target_expr
            .as_literal()
            .and_then(|l| l.as_str())
            .ok_or_else(|| {
                crate::error::PlannerError::invalid_operation(
                    "convert_timezone target timezone argument must be a string literal",
                )
            })?;
        let source = planner.plan_function_arg(&inputs[1])?.into_inner();

        let args = vec![
            FunctionArg::unnamed(source),
            FunctionArg::named("to_timezone".to_string(), lit(target_str.to_string())),
        ];

        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(ConvertTimeZone)),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Spark-style alias for convert_time_zone with reversed argument order: convert_timezone(target_tz, source_ts)."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["target_timezone", "source_timestamp"]
    }
}

// --- Unix extractor SQL functions (Spark parity) ---

fn build_unix_extractor(
    inputs: &[ast::FunctionArg],
    planner: &crate::planner::SQLPlanner,
    fn_name: &str,
    time_unit: &str,
) -> SQLPlannerResult<ExprRef> {
    if inputs.len() != 1 {
        invalid_operation_err!("{} expects 1 argument, got {}", fn_name, inputs.len());
    }
    let input = planner.plan_function_arg(&inputs[0])?.into_inner();
    Ok(BuiltinScalarFn {
        func: BuiltinScalarFnVariant::Sync(Arc::new(UnixTimestamp)),
        inputs: FunctionArgs::new_unchecked(vec![
            FunctionArg::unnamed(input),
            FunctionArg::named("time_unit".to_string(), lit(time_unit.to_string())),
        ]),
    }
    .into())
}

pub struct SQLUnixSeconds;

impl SQLFunction for SQLUnixSeconds {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        build_unix_extractor(inputs, planner, "unix_seconds", "s")
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the number of seconds since the Unix epoch.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLUnixMillis;

impl SQLFunction for SQLUnixMillis {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        build_unix_extractor(inputs, planner, "unix_millis", "ms")
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the number of milliseconds since the Unix epoch.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLUnixMicros;

impl SQLFunction for SQLUnixMicros {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        build_unix_extractor(inputs, planner, "unix_micros", "us")
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the number of microseconds since the Unix epoch.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLUnixTimestamp;

impl SQLFunction for SQLUnixTimestamp {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        build_unix_extractor(inputs, planner, "unix_timestamp", "s")
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the number of seconds since the Unix epoch (Spark default unit).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLWeekday;

impl SQLFunction for SQLWeekday {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("weekday expects 1 argument, got {}", inputs.len());
        }
        let input = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(DayOfWeek)),
            inputs: FunctionArgs::new_unchecked(vec![FunctionArg::unnamed(input)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the day of the week with Monday=0, Sunday=6 numbering.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}
