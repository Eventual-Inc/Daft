use daft_core::datatypes::TimeUnit;
use daft_functions_temporal::{
    Day, DayOfMonth, DayOfWeek, DayOfYear, Hour, Minute, Month, Quarter, Second, UnixDate,
    WeekOfYear, Year,
};
use daft_schema::dtype::DataType;

use super::{FunctionModule, UnaryFunction};

/// https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions
pub struct DatetimeFunctions;

impl FunctionModule for DatetimeFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_todo_fn("add_months");
        parent.add_todo_fn("convert_timezone");
        parent.add_todo_fn("curdate");
        parent.add_todo_fn("current_date");
        parent.add_todo_fn("current_timestamp");
        parent.add_todo_fn("current_timezone");
        parent.add_todo_fn("date_add");
        parent.add_todo_fn("date_diff");
        parent.add_todo_fn("date_format");
        parent.add_todo_fn("date_from_unix_date");
        parent.add_todo_fn("date_part");
        parent.add_todo_fn("date_sub");
        parent.add_todo_fn("date_trunc");
        parent.add_todo_fn("dateadd");
        parent.add_todo_fn("datediff");
        parent.add_todo_fn("datepart");
        parent.add_fn("day", Day);
        parent.add_fn("dayofmonth", DayOfMonth);
        parent.add_fn("dayofweek", DayOfWeek);

        parent.add_fn("dayofyear", DayOfYear);
        parent.add_todo_fn("extract");
        parent.add_todo_fn("from_unixtime");
        parent.add_todo_fn("from_utc_timestamp");
        parent.add_fn("hour", Hour);
        parent.add_todo_fn("last_day");
        parent.add_todo_fn("localtimestamp");
        parent.add_todo_fn("make_date");
        parent.add_todo_fn("make_dt_interval");
        parent.add_todo_fn("make_interval");
        parent.add_todo_fn("make_timestamp");
        parent.add_todo_fn("make_timestamp_ltz");
        parent.add_todo_fn("make_timestamp_ntz");
        parent.add_todo_fn("make_ym_interval");
        parent.add_fn("minute", Minute);
        parent.add_fn("month", Month);
        parent.add_todo_fn("months_between");
        parent.add_todo_fn("next_day");
        parent.add_todo_fn("now");
        parent.add_fn("quarter", Quarter);
        parent.add_fn("second", Second);
        parent.add_todo_fn("session_window");
        parent.add_todo_fn("timestamp_micros");
        parent.add_todo_fn("timestamp_millis");
        parent.add_todo_fn("timestamp_seconds");
        parent.add_fn("to_date", UnaryFunction(|arg| arg.cast(&DataType::Date)));
        parent.add_fn(
            "to_timestamp",
            UnaryFunction(|arg| arg.cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))),
        );
        parent.add_todo_fn("to_timestamp_ltz");
        parent.add_todo_fn("to_timestamp_ntz");
        parent.add_todo_fn("to_unix_timestamp");
        parent.add_todo_fn("to_utc_timestamp");
        parent.add_todo_fn("trunc");
        parent.add_todo_fn("try_to_timestamp");
        parent.add_fn("unix_date", UnixDate);
        parent.add_todo_fn("unix_timestamp");
        parent.add_todo_fn("weekday");
        parent.add_fn("weekofyear", WeekOfYear);
        parent.add_todo_fn("window");
        parent.add_todo_fn("window_time");
        parent.add_fn("year", Year);
    }
}
