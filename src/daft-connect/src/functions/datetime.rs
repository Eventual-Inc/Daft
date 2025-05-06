use daft_core::datatypes::TimeUnit;
use daft_functions::temporal::{
    Day, DayOfMonth, DayOfWeek, DayOfYear, Hour, Minute, Month, Quarter, Second, UnixDate,
    WeekOfYear, Year,
};
use daft_schema::dtype::DataType;

use super::{FunctionModule, UnaryFunction, TODO_FUNCTION};

/// https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions
pub struct DatetimeFunctions;

impl FunctionModule for DatetimeFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("add_months", TODO_FUNCTION);
        parent.add_fn("convert_timezone", TODO_FUNCTION);
        parent.add_fn("curdate", TODO_FUNCTION);
        parent.add_fn("current_date", TODO_FUNCTION);
        parent.add_fn("current_timestamp", TODO_FUNCTION);
        parent.add_fn("current_timezone", TODO_FUNCTION);
        parent.add_fn("date_add", TODO_FUNCTION);
        parent.add_fn("date_diff", TODO_FUNCTION);
        parent.add_fn("date_format", TODO_FUNCTION);
        parent.add_fn("date_from_unix_date", TODO_FUNCTION);
        parent.add_fn("date_part", TODO_FUNCTION);
        parent.add_fn("date_sub", TODO_FUNCTION);
        parent.add_fn("date_trunc", TODO_FUNCTION);
        parent.add_fn("dateadd", TODO_FUNCTION);
        parent.add_fn("datediff", TODO_FUNCTION);
        parent.add_fn("datepart", TODO_FUNCTION);
        parent.add_fn("day", Day);
        parent.add_fn("dayofmonth", DayOfMonth);
        parent.add_fn("dayofweek", DayOfWeek);

        parent.add_fn("dayofyear", DayOfYear);
        parent.add_fn("extract", TODO_FUNCTION);
        parent.add_fn("from_unixtime", TODO_FUNCTION);
        parent.add_fn("from_utc_timestamp", TODO_FUNCTION);
        parent.add_fn("hour", Hour);
        parent.add_fn("last_day", TODO_FUNCTION);
        parent.add_fn("localtimestamp", TODO_FUNCTION);
        parent.add_fn("make_date", TODO_FUNCTION);
        parent.add_fn("make_dt_interval", TODO_FUNCTION);
        parent.add_fn("make_interval", TODO_FUNCTION);
        parent.add_fn("make_timestamp", TODO_FUNCTION);
        parent.add_fn("make_timestamp_ltz", TODO_FUNCTION);
        parent.add_fn("make_timestamp_ntz", TODO_FUNCTION);
        parent.add_fn("make_ym_interval", TODO_FUNCTION);
        parent.add_fn("minute", Minute);
        parent.add_fn("month", Month);
        parent.add_fn("months_between", TODO_FUNCTION);
        parent.add_fn("next_day", TODO_FUNCTION);
        parent.add_fn("now", TODO_FUNCTION);
        parent.add_fn("quarter", Quarter);
        parent.add_fn("second", Second);
        parent.add_fn("session_window", TODO_FUNCTION);
        parent.add_fn("timestamp_micros", TODO_FUNCTION);
        parent.add_fn("timestamp_millis", TODO_FUNCTION);
        parent.add_fn("timestamp_seconds", TODO_FUNCTION);
        parent.add_fn("to_date", UnaryFunction(|arg| arg.cast(&DataType::Date)));
        parent.add_fn(
            "to_timestamp",
            UnaryFunction(|arg| arg.cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))),
        );
        parent.add_fn("to_timestamp_ltz", TODO_FUNCTION);
        parent.add_fn("to_timestamp_ntz", TODO_FUNCTION);
        parent.add_fn("to_unix_timestamp", TODO_FUNCTION);
        parent.add_fn("to_utc_timestamp", TODO_FUNCTION);
        parent.add_fn("trunc", TODO_FUNCTION);
        parent.add_fn("try_to_timestamp", TODO_FUNCTION);
        parent.add_fn("unix_date", UnixDate);
        parent.add_fn("unix_timestamp", TODO_FUNCTION);
        parent.add_fn("weekday", TODO_FUNCTION);
        parent.add_fn("weekofyear", WeekOfYear);
        parent.add_fn("window", TODO_FUNCTION);
        parent.add_fn("window_time", TODO_FUNCTION);
        parent.add_fn("year", Year);
    }
}
