use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use common_error::{DaftError, DaftResult};
use daft_arrow::{
    self,
    array::{Array, PrimitiveArray},
    compute::arithmetics::{
        ArraySub,
        time::{add_interval, mul_interval, sub_interval},
    },
    datatypes::ArrowDataType,
    types::months_days_ns,
};
use daft_schema::time_unit::{
    ParsedTimezone, datetime_to_timestamp, naive_datetime_to_timestamp, naive_local_to_timestamp,
    parse_timezone, timestamp_to_datetime, timestamp_to_naive_datetime, timestamp_to_naive_local,
};

use super::as_arrow::AsArrow;
use crate::{
    array::prelude::*,
    datatypes::{IntervalArray, prelude::*},
};

fn process_interval(interval: &str, timeunit: TimeUnit) -> DaftResult<i64> {
    let (count_str, unit) = interval.split_once(' ').ok_or_else(|| {
        DaftError::ValueError(format!(
            "Invalid interval string: {interval}. Expected format: <count> <unit>"
        ))
    })?;

    let count = count_str
        .parse::<i64>()
        .map_err(|e| DaftError::ValueError(format!("Invalid interval count: {e}")))?;

    let duration = match unit {
        "week" | "weeks" => Duration::weeks(count),
        "day" | "days" => Duration::days(count),
        "hour" | "hours" => Duration::hours(count),
        "minute" | "minutes" => Duration::minutes(count),
        "second" | "seconds" => Duration::seconds(count),
        "millisecond" | "milliseconds" => Duration::milliseconds(count),
        "microsecond" | "microseconds" => Duration::microseconds(count),
        "nanosecond" | "nanoseconds" => Duration::nanoseconds(count),
        _ => {
            return Err(DaftError::ValueError(format!(
                "Invalid interval unit: {unit}. Expected one of: week, day, hour, minute, second, millisecond, microsecond, nanosecond"
            )));
        }
    };

    match timeunit {
        TimeUnit::Seconds => Ok(duration.num_seconds()),
        TimeUnit::Milliseconds => Ok(duration.num_milliseconds()),
        TimeUnit::Microseconds => {
            duration
                .num_microseconds()
                .ok_or(DaftError::ValueError(format!(
                    "Interval is out of range for microseconds: {interval}"
                )))
        }
        TimeUnit::Nanoseconds => duration
            .num_nanoseconds()
            .ok_or(DaftError::ValueError(format!(
                "Interval is out of range for nanoseconds: {interval}"
            ))),
    }
}

impl DateArray {
    pub fn day(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let day_arr = daft_arrow::compute::temporal::day(&input_array)?;
        Ok((self.name(), Box::new(day_arr)).into())
    }

    pub fn month(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let month_arr = daft_arrow::compute::temporal::month(&input_array)?;
        Ok((self.name(), Box::new(month_arr)).into())
    }

    pub fn quarter(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let month_arr = daft_arrow::compute::temporal::month(&input_array)?;
        let quarter_arr = month_arr
            .into_iter()
            .map(|opt_month| opt_month.map(|month_val| month_val.div_ceil(3)))
            .collect();
        Ok((self.name(), Box::new(quarter_arr)).into())
    }

    pub fn year(&self) -> DaftResult<Int32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let year_arr = daft_arrow::compute::temporal::year(&input_array)?;
        Ok((self.name(), Box::new(year_arr)).into())
    }

    pub fn day_of_week(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let day_arr = daft_arrow::compute::temporal::weekday(&input_array)?;
        Ok((self.name(), Box::new(day_arr.sub(&1))).into())
    }

    pub fn day_of_month(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let day_arr = daft_arrow::compute::temporal::day_of_month(&input_array)?;
        Ok((self.name(), Box::new(day_arr)).into())
    }

    pub fn day_of_year(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let ordinal_day_arr = daft_arrow::compute::temporal::day_of_year(&input_array)?;
        Ok((self.name(), Box::new(ordinal_day_arr)).into())
    }

    pub fn week_of_year(&self) -> DaftResult<UInt32Array> {
        let input_array = self.physical.as_arrow2().clone().to(ArrowDataType::Date32);
        let day_arr = daft_arrow::compute::temporal::week_of_year(&input_array)?;
        Ok((self.name(), Box::new(day_arr)).into())
    }
}

impl TimestampArray {
    pub fn date(&self) -> DaftResult<DateArray> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = *timeunit;
        let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let physical: Int32Array = match tz {
            Some(tz) => {
                let tz_parsed = parse_timezone(tz)?;
                self.physical
                    .into_iter()
                    .map(|ts| {
                        ts.map(|ts| {
                            let local = timestamp_to_naive_local(*ts, tu, &tz_parsed);
                            (local.date() - epoch_date).num_days() as i32
                        })
                    })
                    .collect()
            }
            None => self
                .physical
                .into_iter()
                .map(|ts| {
                    ts.map(|ts| {
                        let naive = timestamp_to_naive_datetime(*ts, tu);
                        (naive.date() - epoch_date).num_days() as i32
                    })
                })
                .collect(),
        };
        Ok(DateArray::new(
            Field::new(self.name(), DataType::Date),
            physical.rename(self.name()),
        ))
    }

    pub fn time(&self, timeunit_for_cast: &TimeUnit) -> DaftResult<TimeArray> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = *timeunit;
        if !matches!(
            timeunit_for_cast,
            TimeUnit::Microseconds | TimeUnit::Nanoseconds
        ) {
            return Err(DaftError::ValueError(format!(
                "Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"
            )));
        }
        let tz_parsed = tz.as_deref().map(parse_timezone).transpose()?;
        let physical: Int64Array = self
            .physical
            .into_iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = match &tz_parsed {
                        Some(tz) => timestamp_to_naive_local(*ts, tu, tz).time(),
                        None => timestamp_to_naive_datetime(*ts, tu).time(),
                    };
                    let time_delta = naive_time - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                    match timeunit_for_cast {
                        TimeUnit::Microseconds => time_delta.num_microseconds().unwrap(),
                        TimeUnit::Nanoseconds => time_delta.num_nanoseconds().unwrap(),
                        _ => unreachable!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"),
                    }
                })
            })
            .collect();
        Ok(TimeArray::new(
            Field::new(self.name(), DataType::Time(*timeunit_for_cast)),
            physical.rename(self.name()),
        ))
    }

    pub fn truncate(&self, interval: &str, relative_to: Option<i64>) -> DaftResult<Self> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let duration = process_interval(interval, *timeunit)?;
        let tz_parsed = tz.as_deref().map(parse_timezone).transpose()?;

        fn truncate_single_ts<T>(
            ts: i64,
            tu: TimeUnit,
            tz: Option<T>,
            duration: i64,
            relative_to: Option<i64>,
        ) -> DaftResult<i64>
        where
            T: chrono::TimeZone,
        {
            // To truncate a timestamp, we need to subtract off the truncate_by amount, which is essentially the amount of time past the last interval boundary.
            // We can calculate this by taking the modulo of the timestamp with the duration.
            match tz {
                Some(tz) => {
                    let original_dt = timestamp_to_datetime(ts, tu, &tz);
                    let naive_ts = naive_datetime_to_timestamp(original_dt.naive_local(), tu)?;
                    let mut truncate_by_amount = match relative_to {
                        Some(rt) => {
                            let dt = timestamp_to_datetime(rt, tu, &tz);
                            let naive_rt = naive_datetime_to_timestamp(dt.naive_local(), tu)?;
                            (naive_ts - naive_rt) % duration
                        }
                        None => naive_ts % duration,
                    };
                    if truncate_by_amount < 0 {
                        truncate_by_amount += duration;
                    }
                    let truncate_by_duration = match tu {
                        TimeUnit::Seconds => Duration::seconds(truncate_by_amount),
                        TimeUnit::Milliseconds => Duration::milliseconds(truncate_by_amount),
                        TimeUnit::Microseconds => Duration::microseconds(truncate_by_amount),
                        TimeUnit::Nanoseconds => Duration::nanoseconds(truncate_by_amount),
                    };
                    let truncated_dt = original_dt - truncate_by_duration;
                    datetime_to_timestamp(truncated_dt, tu)
                }
                None => {
                    let mut truncate_by_amount = match relative_to {
                        Some(rt) => (ts - rt) % duration,
                        None => ts % duration,
                    };
                    if truncate_by_amount < 0 {
                        truncate_by_amount += duration;
                    }
                    Ok(ts - truncate_by_amount)
                }
            }
        }

        let physical: Int64Array = self
            .physical
            .into_iter()
            .map(|ts| {
                ts.map(|ts| match tz_parsed {
                    Some(ParsedTimezone::Fixed(offset)) => {
                        truncate_single_ts(*ts, *timeunit, Some(offset), duration, relative_to)
                    }
                    Some(ParsedTimezone::Tz(tz)) => {
                        truncate_single_ts(*ts, *timeunit, Some(tz), duration, relative_to)
                    }
                    None => truncate_single_ts(
                        *ts,
                        *timeunit,
                        None::<chrono_tz::Tz>,
                        duration,
                        relative_to,
                    ),
                })
                .transpose()
            })
            .collect::<DaftResult<_>>()?;

        Ok(Self::new(
            Field::new(self.name(), self.data_type().clone()),
            physical.rename(self.name()),
        ))
    }

    pub fn convert_time_zone(
        &self,
        to_timezone: &str,
        from_timezone: Option<&str>,
    ) -> DaftResult<Self> {
        let DataType::Timestamp(time_unit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };

        parse_timezone(to_timezone)?;

        if tz.is_some() {
            return Ok(Self::new(
                Field::new(
                    self.name(),
                    DataType::Timestamp(*time_unit, Some(to_timezone.to_string())),
                ),
                self.physical.clone(),
            ));
        }

        let Some(from_tz) = from_timezone else {
            return Err(DaftError::ValueError(
                "from_timezone must be provided for timestamps without a timezone".to_string(),
            ));
        };

        let from_tz_parsed = parse_timezone(from_tz)?;

        let physical: Int64Array = self
            .physical
            .into_iter()
            .map(|ts| {
                ts.map(|ts_val| {
                    let naive = timestamp_to_naive_datetime(*ts_val, *time_unit);
                    naive_local_to_timestamp(naive, *time_unit, &from_tz_parsed, from_tz)
                })
                .transpose()
            })
            .collect::<DaftResult<_>>()?;

        Ok(Self::new(
            Field::new(
                self.name(),
                DataType::Timestamp(*time_unit, Some(to_timezone.to_string())),
            ),
            physical.rename(self.name()),
        ))
    }

    pub fn replace_time_zone(&self, timezone: Option<&str>) -> DaftResult<Self> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };

        if tz.as_deref() == timezone {
            return Ok(self.clone());
        }

        let tz_in = tz.as_deref().map(parse_timezone).transpose()?;
        let tz_out = timezone.map(parse_timezone).transpose()?;

        let physical: Int64Array = self
            .physical
            .into_iter()
            .map(|ts| {
                ts.map(|ts_val| {
                    let naive_local = match &tz_in {
                        Some(tz_in) => timestamp_to_naive_local(*ts_val, *timeunit, tz_in),
                        None => timestamp_to_naive_datetime(*ts_val, *timeunit),
                    };
                    match &tz_out {
                        Some(tz_out) => naive_local_to_timestamp(
                            naive_local,
                            *timeunit,
                            tz_out,
                            timezone.expect("timezone checked above"),
                        ),
                        None => {
                            let datetime = naive_local.and_utc();
                            datetime_to_timestamp(datetime, *timeunit)
                        }
                    }
                })
                .transpose()
            })
            .collect::<DaftResult<_>>()?;

        Ok(Self::new(
            Field::new(
                self.name(),
                DataType::Timestamp(*timeunit, timezone.map(|tz| tz.to_string())),
            ),
            physical.rename(self.name()),
        ))
    }

    pub fn add_interval(&self, interval: &IntervalArray) -> DaftResult<Self> {
        self.interval_helper(interval, add_interval)
    }

    pub fn sub_interval(&self, interval: &IntervalArray) -> DaftResult<Self> {
        self.interval_helper(interval, sub_interval)
    }

    fn interval_helper<
        F: FnOnce(
            &PrimitiveArray<i64>,
            &PrimitiveArray<months_days_ns>,
        ) -> Result<PrimitiveArray<i64>, daft_arrow::error::Error>,
    >(
        &self,
        interval: &IntervalArray,
        f: F,
    ) -> DaftResult<Self> {
        let arrow_interval = interval.as_arrow2();

        let arrow_type = self.data_type().to_arrow2()?;
        let mut arrow_timestamp = self.physical.as_arrow2().clone();

        // `f` expect the inner type to be a timestamp
        arrow_timestamp.change_type(arrow_type);

        let mut physical_res = f(&arrow_timestamp, arrow_interval)?;
        // but daft expects the inner type to be an int64
        // This is because we have our own logical wrapper around the arrow array,
        // so the physical array's dtype should always match the physical type
        physical_res.change_type(ArrowDataType::Int64);

        let physical_field = Arc::new(Field::new(self.name(), DataType::Int64));

        Ok(Self::new(
            self.field.clone(),
            Int64Array::new(physical_field, Box::new(physical_res))?,
        ))
    }

    pub fn day_of_month(&self) -> DaftResult<UInt32Array> {
        let (tu, tz) = match self.data_type() {
            DataType::Timestamp(time_unit, tz) => (time_unit.to_arrow2(), tz.clone()),
            _ => unreachable!("TimestampArray must have Timestamp datatype"),
        };
        let input_array = self
            .physical
            .as_arrow2()
            .clone()
            .to(ArrowDataType::Timestamp(tu, tz));

        let ordinal_day_arr = daft_arrow::compute::temporal::day_of_month(&input_array)?;
        Ok((self.name(), Box::new(ordinal_day_arr)).into())
    }

    pub fn day_of_year(&self) -> DaftResult<UInt32Array> {
        let (tu, tz) = match self.data_type() {
            DataType::Timestamp(time_unit, tz) => (time_unit.to_arrow2(), tz.clone()),
            _ => unreachable!("TimestampArray must have Timestamp datatype"),
        };
        let input_array = self
            .physical
            .as_arrow2()
            .clone()
            .to(ArrowDataType::Timestamp(tu, tz));

        let ordinal_day_arr = daft_arrow::compute::temporal::day_of_year(&input_array)?;
        Ok((self.name(), Box::new(ordinal_day_arr)).into())
    }

    pub fn week_of_year(&self) -> DaftResult<UInt32Array> {
        let (tu, tz) = match self.data_type() {
            DataType::Timestamp(time_unit, tz) => (time_unit.to_arrow2(), tz.clone()),
            _ => unreachable!("TimestampArray must have Timestamp datatype"),
        };
        let input_array = self
            .physical
            .as_arrow2()
            .clone()
            .to(ArrowDataType::Timestamp(tu, tz));

        let ordinal_day_arr = daft_arrow::compute::temporal::week_of_year(&input_array)?;
        Ok((self.name(), Box::new(ordinal_day_arr)).into())
    }

    pub fn unix_date(&self) -> DaftResult<UInt64Array> {
        const UNIX_EPOCH_DATE: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();
        let DataType::Timestamp(tu, tz) = self.data_type() else {
            unreachable!("TimestampArray must have Timestamp datatype")
        };
        let unix_seconds_arr = self
            .physical
            .as_arrow2()
            .clone()
            .to(ArrowDataType::Timestamp(tu.to_arrow2(), tz.clone()));
        let date_arrow = unix_seconds_arr
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let datetime = timestamp_to_datetime(*ts, *tu, &chrono::Utc);
                    datetime
                        .date_naive()
                        .signed_duration_since(UNIX_EPOCH_DATE)
                        .num_days() as u64
                })
            })
            .collect::<Vec<_>>();

        UInt64Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt64)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }
}

impl IntervalArray {
    pub fn mul(&self, factor: &Int32Array) -> DaftResult<Self> {
        let arrow_interval = self.as_arrow2();
        let arrow_factor = factor.as_arrow2();
        let result = mul_interval(arrow_interval, arrow_factor)?;
        Self::new(self.field.clone(), Box::new(result))
    }
}

impl TimeArray {
    pub fn hour(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow2();
        let DataType::Time(time_unit) = self.data_type() else {
            unreachable!("TimeArray must have Time datatype");
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = timestamp_to_datetime(*ts, *time_unit, &chrono::Utc).time();
                    naive_time.hour() as u32
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }

    pub fn minute(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow2();
        let DataType::Time(time_unit) = self.data_type() else {
            unreachable!("TimeArray must have Time datatype");
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = timestamp_to_datetime(*ts, *time_unit, &chrono::Utc).time();
                    naive_time.minute() as u32
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }

    pub fn second(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow2();
        let DataType::Time(time_unit) = self.data_type() else {
            unreachable!("TimeArray must have Time datatype")
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = timestamp_to_datetime(*ts, *time_unit, &chrono::Utc).time();
                    naive_time.second() as u32
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }

    pub fn millisecond(&self) -> DaftResult<UInt32Array> {
        const NANOS_PER_MILLI: u32 = 1_000_000;
        let physical = self.physical.as_arrow2();
        let DataType::Time(time_unit) = self.data_type() else {
            unreachable!("TimeArray must have Time datatype");
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = timestamp_to_datetime(*ts, *time_unit, &chrono::Utc).time();
                    naive_time.nanosecond() / NANOS_PER_MILLI
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }

    pub fn microsecond(&self) -> DaftResult<UInt32Array> {
        const NANOS_PER_MICRO: u32 = 1_000;
        let physical = self.physical.as_arrow2();
        let DataType::Time(time_unit) = self.data_type() else {
            unreachable!("TimeArray must have Time datatype");
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = timestamp_to_datetime(*ts, *time_unit, &chrono::Utc).time();
                    naive_time.nanosecond() / NANOS_PER_MICRO
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }

    pub fn nanosecond(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow2();
        let DataType::Time(time_unit) = self.data_type() else {
            unreachable!("TimeArray must have Time datatype");
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time = timestamp_to_datetime(*ts, *time_unit, &chrono::Utc).time();
                    naive_time.nanosecond()
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }
}
