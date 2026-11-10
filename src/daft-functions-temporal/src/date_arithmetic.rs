use std::{
    ops::{Add, Sub},
    sync::Arc,
};

use arrow_array::{Date32Array, Float64Array};
use chrono::{Datelike, Months, NaiveDate, NaiveDateTime, Timelike};
use daft_core::{datatypes::Int32Type, prelude::AsArrow};
use daft_dsl::functions::prelude::*;

use crate::broadcast_to_common_len;

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
        let [date_series, months_i32] = broadcast_to_common_len(
            self.name(),
            [input.cast(&DataType::Date)?, months.cast(&DataType::Int32)?],
        )?;
        let date_arr = date_series.date()?;
        let dates = date_arr.as_arrow()?;
        let months_arr = months_i32
            .downcast::<daft_core::array::DataArray<Int32Type>>()?
            .as_arrow()?;

        let values: Vec<Option<i32>> = dates
            .iter()
            .zip(months_arr.iter())
            .map(|(opt_days, opt_months)| match (opt_days, opt_months) {
                (Some(days), Some(m)) => shift_months(days_to_date(days), m).map(date_to_days),
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

// Number of *complete* months between two dates, matching Java's
// ChronoUnit.MONTHS.between semantics (truncate toward zero). The naive
// `(year_diff * 12) + month_diff` is one too large when chrono's end-of-month
// clamping pushes `start + m_raw months` past `end` — e.g. (2021-02-15,
// 2020-12-30): start + 2 months clamps to 2021-02-28 which is after 2021-02-15,
// so only 1 full month fits. The mirror case applies in the negative direction.
fn months_diff_floor(end: NaiveDate, start: NaiveDate) -> i32 {
    let m_raw = (end.year() - start.year()) * 12 + (end.month() as i32 - start.month() as i32);
    match shift_months(start, m_raw) {
        Some(shifted) if m_raw > 0 && shifted > end => m_raw - 1,
        Some(shifted) if m_raw < 0 && shifted < end => m_raw + 1,
        _ => m_raw,
    }
}

fn months_between_dt(end: NaiveDateTime, start: NaiveDateTime) -> f64 {
    let end_date = end.date();
    let start_date = start.date();

    let same_day = end_date.day() == start_date.day();
    let both_last_day = is_last_day_of_month(end_date) && is_last_day_of_month(start_date);
    if same_day || both_last_day {
        // Both fast-path cases align with ChronoUnit.MONTHS.between, so the
        // naive month delta is exact here.
        let m = (end_date.year() - start_date.year()) * 12
            + (end_date.month() as i32 - start_date.month() as i32);
        return m as f64;
    }

    let months_diff = months_diff_floor(end_date, start_date);

    // Seconds-of-day component (Spark normalizes time-of-day to a per-31-day fraction)
    let seconds_per_day = 86_400.0_f64;
    let t_end =
        end.num_seconds_from_midnight() as f64 + (end.nanosecond() as f64) / 1_000_000_000.0;
    let t_start =
        start.num_seconds_from_midnight() as f64 + (start.nanosecond() as f64) / 1_000_000_000.0;

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
        let [end_ts, start_ts] = broadcast_to_common_len(
            self.name(),
            [end_date.cast(&ts_dtype)?, start_date.cast(&ts_dtype)?],
        )?;

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

#[cfg(test)]
mod tests {
    use daft_core::{prelude::Int32Array, series::IntoSeries};
    use daft_dsl::functions::scalar::EvalContext;

    use super::*;

    fn i32_series(name: &str, values: &[Option<i32>]) -> Series {
        Int32Array::from_iter(Field::new(name, DataType::Int32), values.iter().copied())
            .into_series()
    }

    fn date_series(name: &str, values: &[Option<(i32, u32, u32)>]) -> Series {
        let days: Vec<Option<i32>> = values
            .iter()
            .map(|v| v.map(|(y, m, d)| date_to_days(NaiveDate::from_ymd_opt(y, m, d).unwrap())))
            .collect();
        i32_series(name, &days).cast(&DataType::Date).unwrap()
    }

    fn date_values(series: &Series) -> Vec<Option<i32>> {
        assert_eq!(series.data_type(), &DataType::Date);
        series
            .cast(&DataType::Int32)
            .unwrap()
            .i32()
            .unwrap()
            .into_iter()
            .collect()
    }

    fn call(udf: &dyn ScalarUDF, args: Vec<Series>) -> DaftResult<Series> {
        let row_count = args.iter().map(Series::len).max().unwrap_or(0);
        udf.call(FunctionArgs::new_unnamed(args), &EvalContext { row_count })
    }

    #[test]
    fn test_add_months_broadcasts_literal_months() {
        let dates = date_series(
            "d",
            &[
                Some((2021, 1, 31)),
                Some((2020, 6, 15)),
                Some((2019, 12, 1)),
                None,
            ],
        );
        let result = call(&AddMonths, vec![dates, i32_series("literal", &[Some(1)])]).unwrap();
        let expected = date_series(
            "d",
            &[
                Some((2021, 2, 28)),
                Some((2020, 7, 15)),
                Some((2020, 1, 1)),
                None,
            ],
        );
        assert_eq!(result.name(), "d");
        assert_eq!(date_values(&result), date_values(&expected));
    }

    #[test]
    fn test_add_months_broadcasts_literal_date() {
        let date = date_series("literal", &[Some((2021, 1, 31))]);
        let months = i32_series("m", &[Some(1), Some(-2), None, Some(13)]);
        let result = call(&AddMonths, vec![date, months]).unwrap();
        let expected = date_series(
            "literal",
            &[
                Some((2021, 2, 28)),
                Some((2020, 11, 30)),
                None,
                Some((2022, 2, 28)),
            ],
        );
        assert_eq!(date_values(&result), date_values(&expected));
    }

    #[test]
    fn test_add_months_broadcasts_null_literal() {
        let dates = date_series("d", &[Some((2021, 1, 31)), Some((2020, 6, 15))]);
        let result = call(&AddMonths, vec![dates, i32_series("literal", &[None])]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 2);
    }

    #[test]
    fn test_add_months_mismatched_lengths_errors() {
        let dates = date_series("d", &[Some((2021, 1, 31)), Some((2020, 6, 15))]);
        let months = i32_series("m", &[Some(1), Some(2), Some(3)]);
        assert!(call(&AddMonths, vec![dates, months]).is_err());
    }

    fn f64_values(series: &Series) -> Vec<Option<f64>> {
        series.f64().unwrap().into_iter().collect()
    }

    #[test]
    fn test_months_between_broadcasts_literal_start() {
        let end = date_series(
            "d",
            &[
                Some((2021, 1, 31)),
                Some((2020, 6, 15)),
                Some((2019, 12, 1)),
                None,
            ],
        );
        let start = date_series("literal", &[Some((2019, 1, 1))]);
        let result = call(&MonthsBetween, vec![end, start]).unwrap();
        assert_eq!(result.name(), "d");
        assert_eq!(
            f64_values(&result),
            vec![Some(24.96774194), Some(17.4516129), Some(11.0), None]
        );
    }

    #[test]
    fn test_months_between_broadcasts_literal_end() {
        let end = date_series("literal", &[Some((2019, 1, 1))]);
        let start = date_series("d", &[Some((2018, 1, 1)), Some((2019, 3, 1)), None]);
        let result = call(&MonthsBetween, vec![end, start]).unwrap();
        assert_eq!(f64_values(&result), vec![Some(12.0), Some(-2.0), None]);
    }

    #[test]
    fn test_months_between_mismatched_lengths_errors() {
        let end = date_series("e", &[Some((2019, 1, 1)), Some((2019, 2, 1))]);
        let start = date_series("s", &[Some((2018, 1, 1)), Some((2018, 1, 1)), None]);
        assert!(call(&MonthsBetween, vec![end, start]).is_err());
    }
}
