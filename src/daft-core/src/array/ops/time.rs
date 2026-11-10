use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    compute::{DatePart, date_part},
    datatypes::IntervalMonthDayNano,
    error::ArrowError,
};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime};
use common_error::{DaftError, DaftResult};
use daft_schema::time_unit::{
    ParsedTimezone, datetime_to_timestamp, naive_datetime_to_timestamp, naive_local_to_timestamp,
    parse_timezone, timestamp_to_datetime, timestamp_to_naive_datetime, timestamp_to_naive_local,
};

use super::as_arrow::AsArrow;
use crate::{
    array::prelude::*,
    datatypes::{IntervalArray, prelude::*},
};

/// Parses an interval string of the form `"<count> <unit>"` into a number of nanoseconds.
///
/// The result is an `i128` so that intervals that are finer than, or not a whole multiple of,
/// a timestamp's time unit can be represented exactly, and so that large counts of coarse
/// units (e.g. weeks) cannot overflow.
fn process_interval(interval: &str) -> DaftResult<i128> {
    let (count_str, unit) = interval.split_once(' ').ok_or_else(|| {
        DaftError::ValueError(format!(
            "Invalid interval string: {interval}. Expected format: <count> <unit>"
        ))
    })?;

    let count = count_str
        .parse::<i64>()
        .map_err(|e| DaftError::ValueError(format!("Invalid interval count: {e}")))?;

    let unit_nanos: i128 = match unit {
        "week" | "weeks" => 7 * 24 * 60 * 60 * 1_000_000_000,
        "day" | "days" => 24 * 60 * 60 * 1_000_000_000,
        "hour" | "hours" => 60 * 60 * 1_000_000_000,
        "minute" | "minutes" => 60 * 1_000_000_000,
        "second" | "seconds" => 1_000_000_000,
        "millisecond" | "milliseconds" => 1_000_000,
        "microsecond" | "microseconds" => 1_000,
        "nanosecond" | "nanoseconds" => 1,
        _ => {
            return Err(DaftError::ValueError(format!(
                "Invalid interval unit: {unit}. Expected one of: week, day, hour, minute, second, millisecond, microsecond, nanosecond"
            )));
        }
    };

    if count <= 0 {
        return Err(DaftError::ValueError(format!(
            "Invalid interval: {interval}. The interval must be positive"
        )));
    }

    // Cannot overflow: |count| < 2^63 and unit_nanos < 2^50.
    Ok(i128::from(count) * unit_nanos)
}

fn nanos_per_unit(timeunit: TimeUnit) -> i128 {
    match timeunit {
        TimeUnit::Seconds => 1_000_000_000,
        TimeUnit::Milliseconds => 1_000_000,
        TimeUnit::Microseconds => 1_000,
        TimeUnit::Nanoseconds => 1,
    }
}

/// Truncates the timestamp `ts` (in `timeunit`) down to the grid of `interval_nanos`-wide buckets
/// anchored at `origin` (in `timeunit`), returning the truncated timestamp in `timeunit`.
///
/// The arithmetic is done in nanoseconds so that the interval does not need to be a whole multiple
/// of the time unit. If a bucket boundary falls between two representable values of `timeunit`,
/// the result is floored to the closest representable value below it. Values before the origin
/// are floored (not truncated toward zero).
fn truncate_to_interval(
    ts: i64,
    origin: i64,
    timeunit: TimeUnit,
    interval_nanos: i128,
) -> DaftResult<i64> {
    let unit_nanos = nanos_per_unit(timeunit);
    let ts_nanos = i128::from(ts) * unit_nanos;
    let origin_nanos = i128::from(origin) * unit_nanos;
    let truncated_nanos = ts_nanos - (ts_nanos - origin_nanos).rem_euclid(interval_nanos);
    i64::try_from(truncated_nanos.div_euclid(unit_nanos)).map_err(|_| {
        DaftError::ValueError(format!(
            "Truncated timestamp is out of range for time unit {timeunit}"
        ))
    })
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
        let field = Field::new(self.name(), DataType::Int64);
        let tz_parsed = tz.as_deref().map(parse_timezone).transpose()?;
        let time_physical = Int64Array::from_iter(
            field,
            physical.iter().map(|ts| {
                ts.map(|ts| {
                    let naive_time = match &tz_parsed {
                        Some(tz) => timestamp_to_naive_local(ts, *timeunit, tz).time(),
                        None => timestamp_to_naive_datetime(ts, *timeunit).time(),
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
        let interval_nanos = process_interval(interval)?;
        let tz_parsed = tz.as_deref().map(parse_timezone).transpose()?;

        fn truncate_single_ts<T>(
            ts: i64,
            tu: TimeUnit,
            tz: Option<T>,
            interval_nanos: i128,
            relative_to: Option<i64>,
        ) -> DaftResult<i64>
        where
            T: chrono::TimeZone,
        {
            match tz {
                Some(tz) => {
                    // Truncate the naive local time, then shift the original (time zone aware)
                    // datetime back by the same amount.
                    let original_dt = timestamp_to_datetime(ts, tu, &tz);
                    let naive_ts = naive_datetime_to_timestamp(original_dt.naive_local(), tu)?;
                    let naive_origin = match relative_to {
                        Some(rt) => {
                            let rt_dt = timestamp_to_datetime(rt, tu, &tz);
                            naive_datetime_to_timestamp(rt_dt.naive_local(), tu)?
                        }
                        None => 0,
                    };
                    let truncated_naive_ts =
                        truncate_to_interval(naive_ts, naive_origin, tu, interval_nanos)?;
                    let out_of_range = || {
                        DaftError::ValueError(format!(
                            "Truncated timestamp is out of range for time unit {tu}"
                        ))
                    };
                    let truncate_by_amount = naive_ts
                        .checked_sub(truncated_naive_ts)
                        .ok_or_else(out_of_range)?;
                    let truncate_by_duration = match tu {
                        TimeUnit::Seconds => Duration::try_seconds(truncate_by_amount),
                        TimeUnit::Milliseconds => Duration::try_milliseconds(truncate_by_amount),
                        TimeUnit::Microseconds => Some(Duration::microseconds(truncate_by_amount)),
                        TimeUnit::Nanoseconds => Some(Duration::nanoseconds(truncate_by_amount)),
                    }
                    .ok_or_else(out_of_range)?;
                    let truncated_dt = original_dt
                        .checked_sub_signed(truncate_by_duration)
                        .ok_or_else(out_of_range)?;
                    datetime_to_timestamp(truncated_dt, tu)
                }
                None => truncate_to_interval(ts, relative_to.unwrap_or(0), tu, interval_nanos),
            }
        }

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in physical {
            match ts {
                None => builder.append_null(),
                Some(ts) => {
                    let truncated_ts = match tz_parsed {
                        Some(ParsedTimezone::Fixed(offset)) => truncate_single_ts(
                            ts,
                            *timeunit,
                            Some(offset),
                            interval_nanos,
                            relative_to,
                        ),
                        Some(ParsedTimezone::Tz(tz)) => {
                            truncate_single_ts(ts, *timeunit, Some(tz), interval_nanos, relative_to)
                        }
                        None => truncate_single_ts(
                            ts,
                            *timeunit,
                            None::<chrono_tz::Tz>,
                            interval_nanos,
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

        let physical = self.physical.as_arrow()?;
        let from_tz_parsed = parse_timezone(from_tz)?;

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in physical {
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
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };

        if tz.as_deref() == timezone {
            return Ok(self.clone());
        }

        let physical = self.physical.as_arrow()?;
        let tz_in = tz.as_deref().map(parse_timezone).transpose()?;
        let tz_out = timezone.map(parse_timezone).transpose()?;

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in physical {
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
        self.interval_op(interval, arrow::compute::kernels::numeric::add)
    }

    pub fn sub_interval(&self, interval: &IntervalArray) -> DaftResult<Self> {
        self.interval_op(interval, arrow::compute::kernels::numeric::sub)
    }

    fn interval_op(
        &self,
        interval: &IntervalArray,
        op: fn(&dyn arrow::array::Datum, &dyn arrow::array::Datum) -> Result<ArrayRef, ArrowError>,
    ) -> DaftResult<Self> {
        let ts_array = self.to_arrow()?;
        let interval_array = interval.to_arrow();
        let result = if interval_array.len() == 1 {
            let scalar = arrow::array::Scalar::new(interval_array);
            op(&ts_array, &scalar)?
        } else {
            op(&ts_array, &interval_array)?
        };
        Self::from_arrow(self.field.clone(), result)
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
        const UNIX_EPOCH_DATE: NaiveDate = DateTime::UNIX_EPOCH.naive_utc().date();
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
        let intervals = self.to_arrow();
        let intervals = intervals
            .as_any()
            .downcast_ref::<arrow::array::IntervalMonthDayNanoArray>()
            .ok_or_else(|| {
                DaftError::TypeError("Failed to downcast to IntervalMonthDayNanoArray".to_string())
            })?;
        let factors = factor.as_arrow()?;
        let mul_op = |iv: IntervalMonthDayNano, f: i32| {
            IntervalMonthDayNano::new(iv.months * f, iv.days * f, iv.nanoseconds * i64::from(f))
        };
        let result: arrow::array::IntervalMonthDayNanoArray = if factors.len() == 1 {
            match factors.iter().next().unwrap() {
                Some(f) => intervals
                    .iter()
                    .map(|iv| iv.map(|iv| mul_op(iv, f)))
                    .collect(),
                None => std::iter::repeat_n(None, intervals.len()).collect(),
            }
        } else {
            intervals
                .iter()
                .zip(factors.iter())
                .map(|(iv, f)| match (iv, f) {
                    (Some(iv), Some(f)) => Some(mul_op(iv, f)),
                    _ => None,
                })
                .collect()
        };
        Self::from_arrow(self.field.clone(), Arc::new(result))
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

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use super::process_interval;
    use crate::{array::ops::as_arrow::AsArrow, prelude::*};

    fn truncate(
        values: &[Option<i64>],
        tu: TimeUnit,
        tz: Option<&str>,
        interval: &str,
        relative_to: Option<i64>,
    ) -> DaftResult<Vec<Option<i64>>> {
        let arr = TimestampArray::new(
            Field::new("ts", DataType::Timestamp(tu, tz.map(str::to_string))),
            Int64Array::from_iter(Field::new("ts", DataType::Int64), values.iter().copied()),
        );
        let truncated = arr.truncate(interval, relative_to)?;
        Ok(truncated.physical.as_arrow()?.iter().collect())
    }

    #[test]
    fn test_process_interval() -> DaftResult<()> {
        assert_eq!(process_interval("1 nanosecond")?, 1);
        assert_eq!(process_interval("2500 nanoseconds")?, 2500);
        assert_eq!(process_interval("3 microseconds")?, 3_000);
        assert_eq!(process_interval("1 week")?, 604_800_000_000_000);
        // Large counts of coarse units must not overflow or panic.
        assert_eq!(
            process_interval("9223372036854775807 weeks")?,
            i128::from(i64::MAX) * 604_800_000_000_000
        );
        for interval in ["0 days", "0 nanoseconds", "-1 days", "-5 microseconds"] {
            let err = process_interval(interval).unwrap_err();
            assert!(
                err.to_string().contains("must be positive"),
                "unexpected error for {interval}: {err}"
            );
        }
        assert!(process_interval("1 year").is_err());
        assert!(process_interval("1").is_err());
        Ok(())
    }

    #[test]
    fn test_truncate_interval_finer_than_time_unit_is_noop() -> DaftResult<()> {
        let values = [Some(49_012), Some(-3), None, Some(0)];
        for interval in ["1 nanosecond", "500 nanoseconds", "1000 nanoseconds"] {
            assert_eq!(
                truncate(&values, TimeUnit::Microseconds, None, interval, None)?,
                values
            );
        }
        let values = [Some(1_614_602_096), Some(-1)];
        assert_eq!(
            truncate(&values, TimeUnit::Seconds, None, "1 millisecond", None)?,
            values
        );
        Ok(())
    }

    #[test]
    fn test_truncate_interval_not_multiple_of_time_unit() -> DaftResult<()> {
        // 2500ns buckets on a microsecond column: boundaries at ..., -2.5, 0, 2.5, 5, ...µs.
        // Boundaries between two representable values are floored.
        assert_eq!(
            truncate(
                &[Some(49_012), Some(49_013), Some(-1), Some(-3), None],
                TimeUnit::Microseconds,
                None,
                "2500 nanoseconds",
                None,
            )?,
            [Some(49_010), Some(49_012), Some(-3), Some(-5), None]
        );
        // 1500ms buckets on a second column.
        assert_eq!(
            truncate(
                &[Some(1), Some(2), Some(3), Some(-1)],
                TimeUnit::Seconds,
                None,
                "1500 milliseconds",
                None,
            )?,
            [Some(0), Some(1), Some(3), Some(-2)]
        );
        Ok(())
    }

    #[test]
    fn test_truncate_relative_to_and_negative_timestamps() -> DaftResult<()> {
        // Buckets aligned to relative_to = 1µs: ..., 1, 3.5, 6, ...µs.
        assert_eq!(
            truncate(
                &[Some(5), Some(6), Some(0)],
                TimeUnit::Microseconds,
                None,
                "2500 nanoseconds",
                Some(1),
            )?,
            [Some(3), Some(6), Some(-2)]
        );
        // Pre-epoch values are floored, not truncated toward zero.
        assert_eq!(
            truncate(
                &[Some(-1), Some(-86_400), Some(-86_401)],
                TimeUnit::Seconds,
                None,
                "1 day",
                None,
            )?,
            [Some(-86_400), Some(-86_400), Some(-172_800)]
        );
        Ok(())
    }

    #[test]
    fn test_truncate_with_time_zone() -> DaftResult<()> {
        // 1970-01-01T00:00:00Z is 09:00 local time in +09:00; truncating to a day gives local
        // midnight, i.e. 1969-12-31T15:00:00Z.
        assert_eq!(
            truncate(
                &[Some(0), None],
                TimeUnit::Seconds,
                Some("+09:00"),
                "1 day",
                None,
            )?,
            [Some(-9 * 3600), None]
        );
        assert_eq!(
            truncate(
                &[Some(49_012), Some(49_013)],
                TimeUnit::Microseconds,
                Some("Europe/Berlin"),
                "2500 nanoseconds",
                None,
            )?,
            [Some(49_010), Some(49_012)]
        );
        assert_eq!(
            truncate(
                &[Some(49_012)],
                TimeUnit::Microseconds,
                Some("Europe/Berlin"),
                "1 nanosecond",
                None,
            )?,
            [Some(49_012)]
        );
        Ok(())
    }

    #[test]
    fn test_truncate_invalid_intervals_error() {
        for interval in ["0 days", "0 nanoseconds", "-1 hours"] {
            for tz in [None, Some("UTC")] {
                let err =
                    truncate(&[Some(1)], TimeUnit::Microseconds, tz, interval, None).unwrap_err();
                assert!(matches!(err, common_error::DaftError::ValueError(_)));
            }
        }
        // Truncating to a huge interval before the origin falls out of range: error, no panic.
        let err = truncate(
            &[Some(-1)],
            TimeUnit::Nanoseconds,
            None,
            "9223372036854775807 weeks",
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range"), "{err}");
    }
}
