use std::sync::Arc;

use arrow_array::{Date32Array, TimestampMicrosecondArray};
use chrono::NaiveDate;
use daft_core::datatypes::TimeUnit;
use daft_dsl::functions::prelude::*;

use crate::broadcast_to_common_len;

const UNIX_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => unreachable!(),
};

// --- MakeDate ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MakeDate;

#[derive(FunctionArgs)]
struct MakeDateArgs<T> {
    year: T,
    month: T,
    day: T,
}

#[typetag::serde]
impl ScalarUDF for MakeDate {
    fn name(&self) -> &'static str {
        "make_date"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let MakeDateArgs { year, month, day } = inputs.try_into()?;
        let [year_i32, month_i32, day_i32] = broadcast_to_common_len(
            self.name(),
            [
                year.cast(&DataType::Int32)?,
                month.cast(&DataType::Int32)?,
                day.cast(&DataType::Int32)?,
            ],
        )?;

        let year_arr = year_i32.i32()?;
        let month_arr = month_i32.i32()?;
        let day_arr = day_i32.i32()?;

        let field_name = year_arr.name().to_string();

        let values: Vec<Option<i32>> = year_arr
            .into_iter()
            .zip(month_arr.into_iter())
            .zip(day_arr.into_iter())
            .map(|((y, m), d)| match (y, m, d) {
                (Some(y), Some(m), Some(d)) => NaiveDate::from_ymd_opt(y, m as u32, d as u32)
                    .map(|date| date.signed_duration_since(UNIX_EPOCH).num_days() as i32),
                _ => None,
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Date32Array::from(values));
        Series::from_arrow(Arc::new(Field::new(field_name, DataType::Date)), arrow_arr)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let MakeDateArgs { year, month, day } = inputs.try_into()?;
        let year_field = year.to_field(schema)?;
        let month_field = month.to_field(schema)?;
        let day_field = day.to_field(schema)?;
        ensure!(
            year_field.dtype.is_integer(),
            TypeError: "Expected integer for year, got {}",
            year_field.dtype
        );
        ensure!(
            month_field.dtype.is_integer(),
            TypeError: "Expected integer for month, got {}",
            month_field.dtype
        );
        ensure!(
            day_field.dtype.is_integer(),
            TypeError: "Expected integer for day, got {}",
            day_field.dtype
        );
        Ok(Field::new(year_field.name, DataType::Date))
    }
}

// --- MakeTimestamp ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MakeTimestamp;

#[derive(FunctionArgs)]
struct MakeTimestampArgs<T> {
    year: T,
    month: T,
    day: T,
    hour: T,
    minute: T,
    second: T,
    #[arg(optional)]
    timezone: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for MakeTimestamp {
    fn name(&self) -> &'static str {
        "make_timestamp"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let MakeTimestampArgs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            timezone,
        } = inputs.try_into()?;

        let [
            year_i32,
            month_i32,
            day_i32,
            hour_i32,
            minute_i32,
            second_f64,
        ] = broadcast_to_common_len(
            self.name(),
            [
                year.cast(&DataType::Int32)?,
                month.cast(&DataType::Int32)?,
                day.cast(&DataType::Int32)?,
                hour.cast(&DataType::Int32)?,
                minute.cast(&DataType::Int32)?,
                second.cast(&DataType::Float64)?,
            ],
        )?;

        let year_arr = year_i32.i32()?;
        let month_arr = month_i32.i32()?;
        let day_arr = day_i32.i32()?;
        let hour_arr = hour_i32.i32()?;
        let minute_arr = minute_i32.i32()?;
        let second_arr = second_f64.f64()?;

        let field_name = year_arr.name().to_string();

        // If a timezone is provided, parse it for offset conversion
        let src_tz: Option<chrono_tz::Tz> = match &timezone {
            Some(tz_str) => Some(tz_str.parse::<chrono_tz::Tz>().map_err(|_| {
                common_error::DaftError::ValueError(format!("Invalid timezone: {tz_str}"))
            })?),
            None => None,
        };

        let values: Vec<Option<i64>> = year_arr
            .into_iter()
            .zip(month_arr.into_iter())
            .zip(day_arr.into_iter())
            .zip(hour_arr.into_iter())
            .zip(minute_arr.into_iter())
            .zip(second_arr.into_iter())
            .map(|(((((y, mo), d), h), mi), s)| match (y, mo, d, h, mi, s) {
                (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) => {
                    let whole_secs = s as u32;
                    let frac_micros_raw = ((s - whole_secs as f64) * 1_000_000.0).round() as u32;
                    let (whole_secs, frac_micros) = if frac_micros_raw >= 1_000_000 {
                        (whole_secs + 1, 0)
                    } else {
                        (whole_secs, frac_micros_raw)
                    };
                    let date = NaiveDate::from_ymd_opt(y, mo as u32, d as u32)?;
                    let time = chrono::NaiveTime::from_hms_micro_opt(
                        h as u32,
                        mi as u32,
                        whole_secs,
                        frac_micros,
                    )?;
                    let naive_dt = chrono::NaiveDateTime::new(date, time);

                    if let Some(tz) = src_tz {
                        use chrono::TimeZone;
                        let local_dt = tz.from_local_datetime(&naive_dt).single()?;
                        Some(local_dt.timestamp_micros())
                    } else {
                        let epoch = chrono::NaiveDateTime::new(UNIX_EPOCH, chrono::NaiveTime::MIN);
                        naive_dt.signed_duration_since(epoch).num_microseconds()
                    }
                }
                _ => None,
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(TimestampMicrosecondArray::from(values));
        Series::from_arrow(
            Arc::new(Field::new(
                field_name,
                DataType::Timestamp(TimeUnit::Microseconds, timezone),
            )),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let MakeTimestampArgs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            timezone,
        } = inputs.try_into()?;
        let year_field = year.to_field(schema)?;
        let month_field = month.to_field(schema)?;
        let day_field = day.to_field(schema)?;
        let hour_field = hour.to_field(schema)?;
        let minute_field = minute.to_field(schema)?;
        let second_field = second.to_field(schema)?;
        ensure!(
            year_field.dtype.is_integer(),
            TypeError: "Expected integer for year, got {}",
            year_field.dtype
        );
        ensure!(
            month_field.dtype.is_integer(),
            TypeError: "Expected integer for month, got {}",
            month_field.dtype
        );
        ensure!(
            day_field.dtype.is_integer(),
            TypeError: "Expected integer for day, got {}",
            day_field.dtype
        );
        ensure!(
            hour_field.dtype.is_integer(),
            TypeError: "Expected integer for hour, got {}",
            hour_field.dtype
        );
        ensure!(
            minute_field.dtype.is_integer(),
            TypeError: "Expected integer for minute, got {}",
            minute_field.dtype
        );
        ensure!(
            second_field.dtype.is_numeric(),
            TypeError: "Expected numeric for second, got {}",
            second_field.dtype
        );
        Ok(Field::new(
            year_field.name,
            DataType::Timestamp(TimeUnit::Microseconds, timezone),
        ))
    }
}

// --- MakeTimestampLtz ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MakeTimestampLtz;

#[derive(FunctionArgs)]
struct MakeTimestampLtzArgs<T> {
    year: T,
    month: T,
    day: T,
    hour: T,
    minute: T,
    second: T,
    #[arg(optional)]
    timezone: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for MakeTimestampLtz {
    fn name(&self) -> &'static str {
        "make_timestamp_ltz"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let MakeTimestampLtzArgs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            timezone,
        } = inputs.try_into()?;

        let [
            year_i32,
            month_i32,
            day_i32,
            hour_i32,
            minute_i32,
            second_f64,
        ] = broadcast_to_common_len(
            self.name(),
            [
                year.cast(&DataType::Int32)?,
                month.cast(&DataType::Int32)?,
                day.cast(&DataType::Int32)?,
                hour.cast(&DataType::Int32)?,
                minute.cast(&DataType::Int32)?,
                second.cast(&DataType::Float64)?,
            ],
        )?;

        let year_arr = year_i32.i32()?;
        let month_arr = month_i32.i32()?;
        let day_arr = day_i32.i32()?;
        let hour_arr = hour_i32.i32()?;
        let minute_arr = minute_i32.i32()?;
        let second_arr = second_f64.f64()?;

        let field_name = year_arr.name().to_string();

        // If a source timezone is provided, parse it for offset conversion
        let src_tz: Option<chrono_tz::Tz> = match &timezone {
            Some(tz_str) => Some(tz_str.parse::<chrono_tz::Tz>().map_err(|_| {
                common_error::DaftError::ValueError(format!("Invalid timezone: {tz_str}"))
            })?),
            None => None,
        };

        let values: Vec<Option<i64>> = year_arr
            .into_iter()
            .zip(month_arr.into_iter())
            .zip(day_arr.into_iter())
            .zip(hour_arr.into_iter())
            .zip(minute_arr.into_iter())
            .zip(second_arr.into_iter())
            .map(|(((((y, mo), d), h), mi), s)| match (y, mo, d, h, mi, s) {
                (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) => {
                    let whole_secs = s as u32;
                    let frac_micros_raw = ((s - whole_secs as f64) * 1_000_000.0).round() as u32;
                    let (whole_secs, frac_micros) = if frac_micros_raw >= 1_000_000 {
                        (whole_secs + 1, 0)
                    } else {
                        (whole_secs, frac_micros_raw)
                    };
                    let date = NaiveDate::from_ymd_opt(y, mo as u32, d as u32)?;
                    let time = chrono::NaiveTime::from_hms_micro_opt(
                        h as u32,
                        mi as u32,
                        whole_secs,
                        frac_micros,
                    )?;
                    let naive_dt = chrono::NaiveDateTime::new(date, time);

                    if let Some(tz) = src_tz {
                        use chrono::TimeZone;
                        let local_dt = tz.from_local_datetime(&naive_dt).single()?;
                        Some(local_dt.timestamp_micros())
                    } else {
                        let epoch = chrono::NaiveDateTime::new(UNIX_EPOCH, chrono::NaiveTime::MIN);
                        naive_dt.signed_duration_since(epoch).num_microseconds()
                    }
                }
                _ => None,
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(TimestampMicrosecondArray::from(values));
        Series::from_arrow(
            Arc::new(Field::new(
                field_name,
                DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string())),
            )),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let MakeTimestampLtzArgs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            ..
        } = inputs.try_into()?;
        let year_field = year.to_field(schema)?;
        let month_field = month.to_field(schema)?;
        let day_field = day.to_field(schema)?;
        let hour_field = hour.to_field(schema)?;
        let minute_field = minute.to_field(schema)?;
        let second_field = second.to_field(schema)?;
        ensure!(
            year_field.dtype.is_integer(),
            TypeError: "Expected integer for year, got {}",
            year_field.dtype
        );
        ensure!(
            month_field.dtype.is_integer(),
            TypeError: "Expected integer for month, got {}",
            month_field.dtype
        );
        ensure!(
            day_field.dtype.is_integer(),
            TypeError: "Expected integer for day, got {}",
            day_field.dtype
        );
        ensure!(
            hour_field.dtype.is_integer(),
            TypeError: "Expected integer for hour, got {}",
            hour_field.dtype
        );
        ensure!(
            minute_field.dtype.is_integer(),
            TypeError: "Expected integer for minute, got {}",
            minute_field.dtype
        );
        ensure!(
            second_field.dtype.is_numeric(),
            TypeError: "Expected numeric for second, got {}",
            second_field.dtype
        );
        Ok(Field::new(
            year_field.name,
            DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string())),
        ))
    }
}

#[cfg(test)]
mod tests {
    use daft_core::{
        prelude::{Float64Array, Int32Array},
        series::IntoSeries,
    };
    use daft_dsl::functions::scalar::EvalContext;

    use super::*;

    fn i32_series(name: &str, values: &[Option<i32>]) -> Series {
        Int32Array::from_iter(Field::new(name, DataType::Int32), values.iter().copied())
            .into_series()
    }

    fn f64_series(name: &str, values: &[Option<f64>]) -> Series {
        Float64Array::from_iter(Field::new(name, DataType::Float64), values.iter().copied())
            .into_series()
    }

    fn call(udf: &dyn ScalarUDF, args: Vec<Series>) -> DaftResult<Series> {
        let row_count = args.iter().map(Series::len).max().unwrap_or(0);
        udf.call(FunctionArgs::new_unnamed(args), &EvalContext { row_count })
    }

    fn days(y: i32, m: u32, d: u32) -> i32 {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .signed_duration_since(UNIX_EPOCH)
            .num_days() as i32
    }

    fn micros(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, s)
            .unwrap()
            .and_utc()
            .timestamp_micros()
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

    fn timestamp_values(series: &Series) -> Vec<Option<i64>> {
        assert!(matches!(series.data_type(), DataType::Timestamp(..)));
        series
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .collect()
    }

    #[test]
    fn test_make_date_columns() {
        let result = call(
            &MakeDate,
            vec![
                i32_series("y", &[Some(2021), Some(2020), None]),
                i32_series("m", &[Some(1), Some(6), Some(3)]),
                i32_series("d", &[Some(31), Some(15), Some(1)]),
            ],
        )
        .unwrap();
        assert_eq!(
            date_values(&result),
            vec![Some(days(2021, 1, 31)), Some(days(2020, 6, 15)), None]
        );
    }

    #[test]
    fn test_make_date_broadcasts_literal_day() {
        let result = call(
            &MakeDate,
            vec![
                i32_series("y", &[Some(2021), Some(2020), Some(2019), None]),
                i32_series("m", &[Some(1), Some(6), Some(12), Some(3)]),
                i32_series("literal", &[Some(1)]),
            ],
        )
        .unwrap();
        assert_eq!(result.name(), "y");
        assert_eq!(
            date_values(&result),
            vec![
                Some(days(2021, 1, 1)),
                Some(days(2020, 6, 1)),
                Some(days(2019, 12, 1)),
                None,
            ]
        );
    }

    #[test]
    fn test_make_date_broadcasts_literal_year_and_month() {
        let result = call(
            &MakeDate,
            vec![
                i32_series("literal", &[Some(2024)]),
                i32_series("literal", &[Some(2)]),
                i32_series("d", &[Some(28), Some(29), Some(30), None]),
            ],
        )
        .unwrap();
        assert_eq!(
            date_values(&result),
            vec![Some(days(2024, 2, 28)), Some(days(2024, 2, 29)), None, None]
        );
    }

    #[test]
    fn test_make_date_broadcasts_null_literal() {
        let result = call(
            &MakeDate,
            vec![
                i32_series("y", &[Some(2021), Some(2020), Some(2019)]),
                i32_series("m", &[Some(1), Some(6), Some(12)]),
                i32_series("literal", &[None]),
            ],
        )
        .unwrap();
        assert_eq!(date_values(&result), vec![None, None, None]);
    }

    #[test]
    fn test_make_date_all_literals() {
        let result = call(
            &MakeDate,
            vec![
                i32_series("y", &[Some(2021)]),
                i32_series("m", &[Some(1)]),
                i32_series("d", &[Some(2)]),
            ],
        )
        .unwrap();
        assert_eq!(date_values(&result), vec![Some(days(2021, 1, 2))]);
    }

    #[test]
    fn test_make_date_empty_column_with_literals() {
        let result = call(
            &MakeDate,
            vec![
                i32_series("y", &[]),
                i32_series("m", &[Some(1)]),
                i32_series("d", &[Some(1)]),
            ],
        )
        .unwrap();
        assert_eq!(date_values(&result), vec![]);
    }

    #[test]
    fn test_make_date_mismatched_lengths_errors() {
        let err = call(
            &MakeDate,
            vec![
                i32_series("y", &[Some(2021), Some(2020), Some(2019)]),
                i32_series("m", &[Some(1), Some(6)]),
                i32_series("d", &[Some(1)]),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("make_date"),
            "unexpected error: {err}"
        );
    }

    fn timestamp_args_with_literal_time() -> Vec<Series> {
        vec![
            i32_series("y", &[Some(2021), Some(2020), Some(2019), None]),
            i32_series("m", &[Some(1), Some(6), Some(12), Some(3)]),
            i32_series("literal", &[Some(1)]),
            i32_series("literal", &[Some(12)]),
            i32_series("literal", &[Some(30)]),
            f64_series("literal", &[Some(15.0)]),
        ]
    }

    #[test]
    fn test_make_timestamp_broadcasts_literals() {
        let result = call(&MakeTimestamp, timestamp_args_with_literal_time()).unwrap();
        assert_eq!(result.name(), "y");
        assert_eq!(
            timestamp_values(&result),
            vec![
                Some(micros(2021, 1, 1, 12, 30, 15)),
                Some(micros(2020, 6, 1, 12, 30, 15)),
                Some(micros(2019, 12, 1, 12, 30, 15)),
                None,
            ]
        );
    }

    #[test]
    fn test_make_timestamp_broadcasts_literal_date_parts() {
        let result = call(
            &MakeTimestamp,
            vec![
                i32_series("literal", &[Some(2024)]),
                i32_series("literal", &[Some(2)]),
                i32_series("literal", &[Some(29)]),
                i32_series("h", &[Some(0), Some(23), None]),
                i32_series("mi", &[Some(0), Some(59), Some(0)]),
                f64_series("s", &[Some(0.0), Some(59.0), Some(0.0)]),
            ],
        )
        .unwrap();
        assert_eq!(
            timestamp_values(&result),
            vec![
                Some(micros(2024, 2, 29, 0, 0, 0)),
                Some(micros(2024, 2, 29, 23, 59, 59)),
                None,
            ]
        );
    }

    #[test]
    fn test_make_timestamp_ltz_broadcasts_literals() {
        let result = call(&MakeTimestampLtz, timestamp_args_with_literal_time()).unwrap();
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string()))
        );
        assert_eq!(
            timestamp_values(&result),
            vec![
                Some(micros(2021, 1, 1, 12, 30, 15)),
                Some(micros(2020, 6, 1, 12, 30, 15)),
                Some(micros(2019, 12, 1, 12, 30, 15)),
                None,
            ]
        );
    }

    #[test]
    fn test_make_timestamp_mismatched_lengths_errors() {
        let mut args = timestamp_args_with_literal_time();
        args[3] = i32_series("h", &[Some(1), Some(2)]);
        assert!(call(&MakeTimestamp, args.clone()).is_err());
        assert!(call(&MakeTimestampLtz, args).is_err());
    }
}
