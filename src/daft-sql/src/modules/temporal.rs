use daft_dsl::ExprRef;
use daft_functions::temporal::*;
use sqlparser::ast::FunctionArg;

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleTemporal;

impl SQLModule for SQLModuleTemporal {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("date", SQLDate, "TODO: Docstring", &["TODO"]);
        parent.add_fn("day", SQLDay, "TODO: Docstring", &["TODO"]);
        parent.add_fn("dayofweek", SQLDayOfWeek, "TODO: Docstring", &["TODO"]);
        parent.add_fn("hour", SQLHour, "TODO: Docstring", &["TODO"]);
        parent.add_fn("minute", SQLMinute, "TODO: Docstring", &["TODO"]);
        parent.add_fn("month", SQLMonth, "TODO: Docstring", &["TODO"]);
        parent.add_fn("second", SQLSecond, "TODO: Docstring", &["TODO"]);
        parent.add_fn("year", SQLYear, "TODO: Docstring", &["TODO"]);
        parent.add_fn("time", SQLTime, "TODO: Docstring", &["TODO"]);

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
        }
    };
}

temporal!(SQLDate, dt_date);
temporal!(SQLDay, dt_day);
temporal!(SQLDayOfWeek, dt_day_of_week);
temporal!(SQLHour, dt_hour);
temporal!(SQLMinute, dt_minute);
temporal!(SQLMonth, dt_month);
temporal!(SQLSecond, dt_second);
temporal!(SQLYear, dt_year);
temporal!(SQLTime, dt_time);
