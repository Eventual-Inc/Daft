use daft_dsl::ExprRef;
use daft_functions::temporal::{
    dt_date, dt_day, dt_day_of_week, dt_day_of_year, dt_hour, dt_microsecond, dt_millisecond,
    dt_minute, dt_month, dt_nanosecond, dt_second, dt_time, dt_year,
};
use sqlparser::ast::FunctionArg;

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{DeprecatedSQLFunction, SQLFunction, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleTemporal;

impl SQLModule for SQLModuleTemporal {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("date", SQLDate);
        parent.add_fn("day", SQLDay);
        parent.add_fn(
            "dayofweek",
            DeprecatedSQLFunction {
                name: "dayofweek",
                replacement: "day_of_week",
                function: &SQLDayOfWeek,
            },
        );
        parent.add_fn("day_of_week", SQLDayOfWeek);
        parent.add_fn("day_of_year", SQLDayOfYear);
        parent.add_fn("hour", SQLHour);
        parent.add_fn("minute", SQLMinute);
        parent.add_fn("month", SQLMonth);
        parent.add_fn("second", SQLSecond);
        parent.add_fn("millisecond", SQLMillisecond);
        parent.add_fn("microsecond", SQLMicrosecond);
        parent.add_fn("nanosecond", SQLNanosecond);
        parent.add_fn("year", SQLYear);
        parent.add_fn("time", SQLTime);

        // TODO: Add truncate
        // Our `dt_truncate` function has vastly different semantics than SQL `DATE_TRUNCATE` function.
    }
}

macro_rules! temporal {
    ($name:ident, $fn_name:ident) => {
        pub struct $name;

        impl SQLFunction for $name {
            fn to_expr(
                &self,
                inputs: &[FunctionArg],
                planner: &crate::planner::SQLPlanner,
            ) -> SQLPlannerResult<ExprRef> {
                match inputs {
                    [input] => {
                        let input = planner.plan_function_arg(input)?;

                        Ok($fn_name(input))
                    }
                    _ => unsupported_sql_err!(
                        "Invalid arguments for {}: '{inputs:?}'",
                        stringify!($fn_name)
                    ),
                }
            }
            fn docstrings(&self, _alias: &str) -> String {
                format!(
                    "Extracts the {} component from a datetime expression.",
                    stringify!($fn_name).replace("dt_", "")
                )
            }

            fn arg_names(&self) -> &'static [&'static str] {
                &["input"]
            }
        }
    };
}

temporal!(SQLDate, dt_date);
temporal!(SQLDay, dt_day);
temporal!(SQLDayOfWeek, dt_day_of_week);
temporal!(SQLDayOfYear, dt_day_of_year);
temporal!(SQLHour, dt_hour);
temporal!(SQLMinute, dt_minute);
temporal!(SQLMonth, dt_month);
temporal!(SQLSecond, dt_second);
temporal!(SQLMillisecond, dt_millisecond);
temporal!(SQLMicrosecond, dt_microsecond);
temporal!(SQLNanosecond, dt_nanosecond);
temporal!(SQLYear, dt_year);
temporal!(SQLTime, dt_time);
