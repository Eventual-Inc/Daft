use std::sync::Arc;

use arrow::compute::{DatePart, date_part};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use common_error::{DaftError, DaftResult};
use daft_arrow::{
    self,
    array::{Array, PrimitiveArray},
    compute::arithmetics::time::{add_interval, mul_interval, sub_interval},
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
    fn date_part_as_uint32(&self, part: DatePart) -> DaftResult<UInt32Array> {
        let date_array = self.to_arrow()?;
        let result = date_part(&*date_array, part)?;
        let result = arrow::compute::cast(&*result, &arrow::datatypes::DataType::UInt32)?;
        UInt32Array::from_arrow(Field::new(self.name(), DataType::UInt32), result)
    }

    pub fn day(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Day)
    }

    pub fn month(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Month)
    }

    pub fn quarter(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Quarter)
    }

    pub fn year(&self) -> DaftResult<Int32Array> {
        let date_array = self.to_arrow()?;
        let result = date_part(&*date_array, DatePart::Year)?;
        Int32Array::from_arrow(Field::new(self.name(), DataType::Int32), result)
    }

    pub fn day_of_week(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::DayOfWeekMonday0)
    }

    pub fn day_of_month(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Day)
    }

    pub fn day_of_year(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::DayOfYear)
    }

    pub fn week_of_year(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Week)
    }
}

impl TimestampArray {
    fn date_part_as_uint32(&self, part: DatePart) -> DaftResult<UInt32Array> {
        let ts_array = self.to_arrow()?;
        let result = date_part(&*ts_array, part)?;
        let result = arrow::compute::cast(&*result, &arrow::datatypes::DataType::UInt32)?;
        UInt32Array::from_arrow(Field::new(self.name(), DataType::UInt32), result)
    }

    pub fn date(&self) -> DaftResult<DateArray> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let physical = self.physical.as_arrow()?;
        let tu = *timeunit;
        let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let field = Field::new(self.name(), DataType::Int32);
        let date_physical = match tz {
            Some(tz) => {
                let tz_parsed = parse_timezone(tz)?;
                Int32Array::from_iter(
                    field,
                    physical.iter().map(|ts| {
                        ts.map(|ts| {
                            let local = timestamp_to_naive_local(ts, tu, &tz_parsed);
                            (local.date() - epoch_date).num_days() as i32
                        })
                    }),
                )
            }
            None => Int32Array::from_iter(
                field,
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        let naive = timestamp_to_naive_datetime(ts, tu);
                        (naive.date() - epoch_date).num_days() as i32
                    })
                }),
            ),
        };
        Ok(DateArray::new(
            Field::new(self.name(), DataType::Date),
            date_physical,
        ))
    }

    pub fn time(&self, timeunit_for_cast: &TimeUnit) -> DaftResult<TimeArray> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        if !matches!(
            timeunit_for_cast,
            TimeUnit::Microseconds | TimeUnit::Nanoseconds
        ) {
            return Err(DaftError::ValueError(format!(
                "Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"
            )));
        }
        let physical = self.physical.as_arrow()?;
        let tu = *timeunit;
        let field = Field::new(self.name(), DataType::Int64);
        let tz_parsed = tz.as_deref().map(parse_timezone).transpose()?;
        let time_physical = Int64Array::from_iter(
            field,
            physical.iter().map(|ts| {
                ts.map(|ts| {
                    let naive_time = match &tz_parsed {
                        Some(tz) => timestamp_to_naive_local(ts, tu, tz).time(),
                        None => timestamp_to_naive_datetime(ts, tu).time(),
                    };
                    let time_delta = naive_time - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                    match timeunit_for_cast {
                        TimeUnit::Microseconds => time_delta.num_microseconds().unwrap(),
                        TimeUnit::Nanoseconds => time_delta.num_nanoseconds().unwrap(),
                        _ => unreachable!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"),
                    }
                })
            }),
        );
        Ok(TimeArray::new(
            Field::new(self.name(), DataType::Time(*timeunit_for_cast)),
            time_physical,
        ))
    }

    pub fn truncate(&self, interval: &str, relative_to: Option<i64>) -> DaftResult<Self> {
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let physical = self.physical.as_arrow()?;
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
            match tz {
                Some(tz) => {
                    let original_dt = timestamp_to_datetime(ts, tu, &tz);
                    let naive_ts = naive_datetime_to_timestamp(original_dt.naive_local(), tu)?;
                    let mut truncate_by_amount = match relative_to {
                        Some(rt) => {
                            let rt_dt = timestamp_to_datetime(rt, tu, &tz);
                            let naive_rt_ts = naive_datetime_to_timestamp(rt_dt.naive_local(), tu)?;
                            (naive_ts - naive_rt_ts) % duration
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

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in &physical {
            match ts {
                None => builder.append_null(),
                Some(ts) => {
                    let truncated_ts = match tz_parsed {
                        Some(ParsedTimezone::Fixed(offset)) => {
                            truncate_single_ts(ts, *timeunit, Some(offset), duration, relative_to)
                        }
                        Some(ParsedTimezone::Tz(tz)) => {
                            truncate_single_ts(ts, *timeunit, Some(tz), duration, relative_to)
                        }
                        None => truncate_single_ts(
                            ts,
                            *timeunit,
                            None::<chrono_tz::Tz>,
                            duration,
                            relative_to,
                        ),
                    };
                    builder.append_value(truncated_ts?);
                }
            }
        }
        let physical = Int64Array::from_arrow(
            Field::new(self.name(), DataType::Int64),
            Arc::new(builder.finish()),
        )?;
        Ok(TimestampArray::new(
            Field::new(self.name(), self.data_type().clone()),
            physical,
        ))
    }

    pub fn convert_time_zone(
        &self,
        to_timezone: &str,
        from_timezone: Option<&str>,
    ) -> DaftResult<Self> {
        let physical = self.physical.as_arrow()?;
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

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in &physical {
            match ts {
                None => builder.append_null(),
                Some(ts_val) => {
                    let naive = timestamp_to_naive_datetime(ts_val, *time_unit);
                    let new_ts =
                        naive_local_to_timestamp(naive, *time_unit, &from_tz_parsed, from_tz)?;
                    builder.append_value(new_ts);
                }
            }
        }
        let physical = Int64Array::from_arrow(
            Field::new(self.name(), DataType::Int64),
            Arc::new(builder.finish()),
        )?;

        Ok(TimestampArray::new(
            Field::new(
                self.name(),
                DataType::Timestamp(*time_unit, Some(to_timezone.to_string())),
            ),
            physical,
        ))
    }

    pub fn replace_time_zone(&self, timezone: Option<&str>) -> DaftResult<Self> {
        let physical = self.physical.as_arrow()?;
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };

        if tz.as_deref() == timezone {
            return Ok(self.clone());
        }

        let tz_in = tz.as_deref().map(parse_timezone).transpose()?;
        let tz_out = timezone.map(parse_timezone).transpose()?;

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in &physical {
            match ts {
                None => builder.append_null(),
                Some(ts_val) => {
                    let naive_local = match &tz_in {
                        Some(tz_in) => timestamp_to_naive_local(ts_val, *timeunit, tz_in),
                        None => timestamp_to_naive_datetime(ts_val, *timeunit),
                    };
                    let new_ts = match &tz_out {
                        Some(tz_out) => naive_local_to_timestamp(
                            naive_local,
                            *timeunit,
                            tz_out,
                            timezone.expect("timezone checked above"),
                        )?,
                        None => {
                            let datetime = naive_local.and_utc();
                            datetime_to_timestamp(datetime, *timeunit)?
                        }
                    };
                    builder.append_value(new_ts);
                }
            }
        }
        let physical = Int64Array::from_arrow(
            Field::new(self.name(), DataType::Int64),
            Arc::new(builder.finish()),
        )?;

        Ok(TimestampArray::new(
            Field::new(
                self.name(),
                DataType::Timestamp(*timeunit, timezone.map(|tz| tz.to_string())),
            ),
            physical,
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
        self.date_part_as_uint32(DatePart::Day)
    }

    pub fn day_of_year(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::DayOfYear)
    }

    pub fn week_of_year(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Week)
    }

    pub fn unix_date(&self) -> DaftResult<UInt64Array> {
        const UNIX_EPOCH_DATE: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();
        let DataType::Timestamp(tu, _tz) = self.data_type() else {
            unreachable!("TimestampArray must have Timestamp datatype")
        };
        let physical = self.physical.as_arrow()?;
        let date_arrow = physical.iter().map(|ts| {
            ts.map(|ts| {
                let datetime = timestamp_to_datetime(ts, *tu, &chrono::Utc);
                datetime
                    .date_naive()
                    .signed_duration_since(UNIX_EPOCH_DATE)
                    .num_days() as u64
            })
        });

        Ok(UInt64Array::from_iter(
            Field::new(self.name(), DataType::UInt64),
            date_arrow,
        ))
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
    fn date_part_as_uint32(&self, part: DatePart) -> DaftResult<UInt32Array> {
        let time_array = self.to_arrow()?;
        let result = date_part(&*time_array, part)?;
        let result = arrow::compute::cast(&*result, &arrow::datatypes::DataType::UInt32)?;
        UInt32Array::from_arrow(Field::new(self.name(), DataType::UInt32), result)
    }

    pub fn hour(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Hour)
    }

    pub fn minute(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Minute)
    }

    pub fn second(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Second)
    }

    pub fn millisecond(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Millisecond)
    }

    pub fn microsecond(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Microsecond)
    }

    pub fn nanosecond(&self) -> DaftResult<UInt32Array> {
        self.date_part_as_uint32(DatePart::Nanosecond)
    }
}
