use std::{
    ops::{Add, Sub},
    sync::Arc,
};

use arrow_array::{Date32Array, Float64Array};
use chrono::{Datelike, Months, NaiveDate, NaiveDateTime, Timelike};
use daft_core::{datatypes::Int32Type, prelude::AsArrow};
use daft_dsl::functions::prelude::*;

const UNIX_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => unreachable!(),
};

fn days_to_date(days: i32) -> NaiveDate {
    UNIX_EPOCH + chrono::Duration::days(days as i64)
}

fn date_to_days(date: NaiveDate) -> i32 {
    date.signed_duration_since(UNIX_EPOCH).num_days() as i32
}

fn shift_months(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    if months >= 0 {
        date.checked_add_months(Months::new(months as u32))
    } else {
        date.checked_sub_months(Months::new(months.unsigned_abs()))
    }
}

// --- DateAdd ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateAdd;

#[derive(FunctionArgs)]
struct DateAddArgs<T> {
    input: T,
    days: T,
}

#[typetag::serde]
impl ScalarUDF for DateAdd {
    fn name(&self) -> &'static str {
        "date_add"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DateAddArgs { input, days } = inputs.try_into()?;
        let date_series = input.cast(&DataType::Date)?;
        let days_i32 = days.cast(&DataType::Int32)?;
        let days_arr = days_i32.downcast::<daft_core::array::DataArray<Int32Type>>()?;
        let result = date_series.date()?.physical.add(days_arr)?;
        result.cast(&DataType::Date)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let DateAddArgs { input, days } = inputs.try_into()?;
        let input_field = input.to_field(schema)?;
        let days_field = days.to_field(schema)?;
        ensure!(
            input_field.dtype == DataType::Date,
            TypeError: "Expected date input, got {}",
            input_field.dtype
        );
        ensure!(
            days_field.dtype.is_integer(),
            TypeError: "Expected integer for days, got {}",
            days_field.dtype
        );
        Ok(Field::new(input_field.name, DataType::Date))
    }
}

// --- DateSub ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateSub;

#[derive(FunctionArgs)]
struct DateSubArgs<T> {
    input: T,
    days: T,
}

#[typetag::serde]
impl ScalarUDF for DateSub {
    fn name(&self) -> &'static str {
        "date_sub"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DateSubArgs { input, days } = inputs.try_into()?;
        let date_series = input.cast(&DataType::Date)?;
        let days_i32 = days.cast(&DataType::Int32)?;
        let days_arr = days_i32.downcast::<daft_core::array::DataArray<Int32Type>>()?;
        let result = date_series.date()?.physical.sub(days_arr)?;
        result.cast(&DataType::Date)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let DateSubArgs { input, days } = inputs.try_into()?;
        let input_field = input.to_field(schema)?;
        let days_field = days.to_field(schema)?;
        ensure!(
            input_field.dtype == DataType::Date,
            TypeError: "Expected date input, got {}",
            input_field.dtype
        );
        ensure!(
            days_field.dtype.is_integer(),
            TypeError: "Expected integer for days, got {}",
            days_field.dtype
        );
        Ok(Field::new(input_field.name, DataType::Date))
    }
}

// --- DateDiff ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateDiff;

#[derive(FunctionArgs)]
struct DateDiffArgs<T> {
    end_date: T,
    start_date: T,
}

#[typetag::serde]
impl ScalarUDF for DateDiff {
    fn name(&self) -> &'static str {
        "date_diff"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DateDiffArgs {
            end_date,
            start_date,
        } = inputs.try_into()?;
        let end_series = end_date.cast(&DataType::Date)?;
        let start_series = start_date.cast(&DataType::Date)?;
        let result = end_series
            .date()?
            .physical
            .sub(&start_series.date()?.physical)?;
        result.cast(&DataType::Int32)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let DateDiffArgs {
            end_date,
            start_date,
        } = inputs.try_into()?;
        let end_field = end_date.to_field(schema)?;
        let start_field = start_date.to_field(schema)?;
        ensure!(
            matches!(end_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp for end_date, got {}",
            end_field.dtype
        );
        ensure!(
            matches!(start_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp for start_date, got {}",
            start_field.dtype
        );
        Ok(Field::new(end_field.name, DataType::Int32))
    }
}

// --- AddMonths ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AddMonths;

#[derive(FunctionArgs)]
struct AddMonthsArgs<T> {
    input: T,
    months: T,
}

#[typetag::serde]
impl ScalarUDF for AddMonths {
    fn name(&self) -> &'static str {
        "add_months"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let AddMonthsArgs { input, months } = inputs.try_into()?;
        let date_series = input.cast(&DataType::Date)?;
        let months_i32 = months.cast(&DataType::Int32)?;
        let date_arr = date_series.date()?;
        let dates = date_arr.as_arrow()?;
        let months_arr = months_i32
            .downcast::<daft_core::array::DataArray<Int32Type>>()?
            .as_arrow()?;

        let values: Vec<Option<i32>> = dates
            .iter()
            .zip(months_arr.iter())
            .map(|(opt_days, opt_months)| match (opt_days, opt_months) {
                (Some(days), Some(m)) => {
                    shift_months(days_to_date(days), m).map(date_to_days)
                }
                _ => None,
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Date32Array::from(values));
        Series::from_arrow(
            Arc::new(Field::new(date_arr.name().to_string(), DataType::Date)),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let AddMonthsArgs { input, months } = inputs.try_into()?;
        let input_field = input.to_field(schema)?;
        let months_field = months.to_field(schema)?;
        ensure!(
            matches!(input_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp input, got {}",
            input_field.dtype
        );
        ensure!(
            months_field.dtype.is_integer(),
            TypeError: "Expected integer for months, got {}",
            months_field.dtype
        );
        Ok(Field::new(input_field.name, DataType::Date))
    }
}

// --- MonthsBetween ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MonthsBetween;

#[derive(FunctionArgs)]
struct MonthsBetweenArgs<T> {
    end_date: T,
    start_date: T,
}

fn is_last_day_of_month(date: NaiveDate) -> bool {
    let (year, month) = (date.year(), date.month());
    let next_month_first = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    };
    match next_month_first {
        Some(d) => date_to_days(d) - date_to_days(date) == 1,
        None => false,
    }
}

fn months_between_dt(end: NaiveDateTime, start: NaiveDateTime) -> f64 {
    let end_date = end.date();
    let start_date = start.date();

    let months_diff =
        (end_date.year() - start_date.year()) * 12 + (end_date.month() as i32 - start_date.month() as i32);

    let same_day = end_date.day() == start_date.day();
    let both_last_day = is_last_day_of_month(end_date) && is_last_day_of_month(start_date);
    if same_day || both_last_day {
        return months_diff as f64;
    }

    // Seconds-of-day component (Spark normalizes time-of-day to a per-31-day fraction)
    let seconds_per_day = 86_400.0_f64;
    let t_end = end.num_seconds_from_midnight() as f64
        + (end.nanosecond() as f64) / 1_000_000_000.0;
    let t_start = start.num_seconds_from_midnight() as f64
        + (start.nanosecond() as f64) / 1_000_000_000.0;

    let day_diff = end_date.day() as i32 - start_date.day() as i32;
    let time_diff_days = (t_end - t_start) / seconds_per_day;

    let raw = months_diff as f64 + (day_diff as f64 + time_diff_days) / 31.0;

    // Spark rounds to 8 decimal places by default.
    (raw * 1e8).round() / 1e8
}

#[typetag::serde]
impl ScalarUDF for MonthsBetween {
    fn name(&self) -> &'static str {
        "months_between"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        use daft_core::prelude::TimeUnit;

        let MonthsBetweenArgs {
            end_date,
            start_date,
        } = inputs.try_into()?;

        // Cast both inputs to a tz-naive microsecond Timestamp. Casting strips the
        // timezone label without shifting the underlying i64, so the comparison is
        // always evaluated in UTC. Daft has no session-timezone concept, so this
        // matches Spark when the session is UTC.
        let ts_dtype = DataType::Timestamp(TimeUnit::Microseconds, None);
        let end_ts = end_date.cast(&ts_dtype)?;
        let start_ts = start_date.cast(&ts_dtype)?;

        let end_ts_arr = end_ts.timestamp()?;
        let start_ts_arr = start_ts.timestamp()?;
        let end_arr = end_ts_arr.as_arrow()?;
        let start_arr = start_ts_arr.as_arrow()?;

        let values: Vec<Option<f64>> = end_arr
            .iter()
            .zip(start_arr.iter())
            .map(|(opt_e, opt_s)| match (opt_e, opt_s) {
                (Some(e_us), Some(s_us)) => {
                    let e_dt = chrono::DateTime::from_timestamp_micros(e_us)?.naive_utc();
                    let s_dt = chrono::DateTime::from_timestamp_micros(s_us)?.naive_utc();
                    Some(months_between_dt(e_dt, s_dt))
                }
                _ => None,
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Float64Array::from(values));
        Series::from_arrow(
            Arc::new(Field::new(end_ts_arr.name().to_string(), DataType::Float64)),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let MonthsBetweenArgs {
            end_date,
            start_date,
        } = inputs.try_into()?;
        let end_field = end_date.to_field(schema)?;
        let start_field = start_date.to_field(schema)?;
        ensure!(
            matches!(end_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp for end_date, got {}",
            end_field.dtype
        );
        ensure!(
            matches!(start_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp for start_date, got {}",
            start_field.dtype
        );
        Ok(Field::new(end_field.name, DataType::Float64))
    }
}
