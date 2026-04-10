use std::sync::Arc;

use arrow_array::{Date32Array, TimestampMicrosecondArray};
use chrono::NaiveDate;
use daft_core::datatypes::TimeUnit;
use daft_dsl::functions::prelude::*;

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
        let year_i32 = year.cast(&DataType::Int32)?;
        let month_i32 = month.cast(&DataType::Int32)?;
        let day_i32 = day.cast(&DataType::Int32)?;

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

        let year_i32 = year.cast(&DataType::Int32)?;
        let month_i32 = month.cast(&DataType::Int32)?;
        let day_i32 = day.cast(&DataType::Int32)?;
        let hour_i32 = hour.cast(&DataType::Int32)?;
        let minute_i32 = minute.cast(&DataType::Int32)?;
        let second_f64 = second.cast(&DataType::Float64)?;

        let year_arr = year_i32.i32()?;
        let month_arr = month_i32.i32()?;
        let day_arr = day_i32.i32()?;
        let hour_arr = hour_i32.i32()?;
        let minute_arr = minute_i32.i32()?;
        let second_arr = second_f64.f64()?;

        let field_name = year_arr.name().to_string();

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
                    let frac_micros = ((s - whole_secs as f64) * 1_000_000.0).round() as u32;
                    let date = NaiveDate::from_ymd_opt(y, mo as u32, d as u32)?;
                    let time = chrono::NaiveTime::from_hms_micro_opt(
                        h as u32,
                        mi as u32,
                        whole_secs,
                        frac_micros,
                    )?;
                    let dt = chrono::NaiveDateTime::new(date, time);
                    let epoch = chrono::NaiveDateTime::new(UNIX_EPOCH, chrono::NaiveTime::MIN);
                    dt.signed_duration_since(epoch).num_microseconds()
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

        let year_i32 = year.cast(&DataType::Int32)?;
        let month_i32 = month.cast(&DataType::Int32)?;
        let day_i32 = day.cast(&DataType::Int32)?;
        let hour_i32 = hour.cast(&DataType::Int32)?;
        let minute_i32 = minute.cast(&DataType::Int32)?;
        let second_f64 = second.cast(&DataType::Float64)?;

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
                    let frac_micros = ((s - whole_secs as f64) * 1_000_000.0).round() as u32;
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
