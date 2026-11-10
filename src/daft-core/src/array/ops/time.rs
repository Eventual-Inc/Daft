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

/// A truncation interval resolved against a time unit once per array, so that the common case
/// (an interval that is a whole multiple of the unit) stays in `i64` arithmetic per value.
#[derive(Clone, Copy)]
enum TruncationGrid {
    /// The interval evenly divides the time unit, so every value is already on the grid.
    Unchanged,
    /// The interval as a whole number of time units.
    Units(i64),
    /// The interval and the time unit in nanoseconds, for any other interval.
    Nanos { interval: i128, unit: i128 },
}

impl TruncationGrid {
    fn new(interval_nanos: i128, timeunit: TimeUnit) -> Self {
        let unit: i128 = match timeunit {
            TimeUnit::Seconds => 1_000_000_000,
            TimeUnit::Milliseconds => 1_000_000,
            TimeUnit::Microseconds => 1_000,
            TimeUnit::Nanoseconds => 1,
        };
        if unit % interval_nanos == 0 {
            return Self::Unchanged;
        }
        match i64::try_from(interval_nanos / unit) {
            Ok(units) if interval_nanos % unit == 0 => Self::Units(units),
            _ => Self::Nanos {
                interval: interval_nanos,
                unit,
            },
        }
    }

    /// Floors `ts` onto the grid anchored at `origin` (both in the time unit), or `None` if the
    /// result is out of range. Values before the origin are floored, not truncated toward zero.
    /// If a grid point falls between two representable values, the lower one is returned.
    fn truncate(self, ts: i64, origin: i64) -> Option<i64> {
        let floor = |ts: i128, offset: i128, interval: i128| ts - offset.rem_euclid(interval);
        match self {
            Self::Unchanged => Some(ts),
            Self::Units(interval) => match ts.checked_sub(origin) {
                Some(offset) => ts.checked_sub(offset.rem_euclid(interval)),
                None => {
                    let offset = i128::from(ts) - i128::from(origin);
                    i64::try_from(floor(ts.into(), offset, interval.into())).ok()
                }
            },
            Self::Nanos { interval, unit } => {
                let ts_nanos = i128::from(ts) * unit;
                let offset = ts_nanos - i128::from(origin) * unit;
                i64::try_from(floor(ts_nanos, offset, interval).div_euclid(unit)).ok()
            }
        }
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
        let grid = TruncationGrid::new(process_interval(interval)?, *timeunit);
        let tz_parsed = tz.as_deref().map(parse_timezone).transpose()?;

        fn truncate_single_ts<T>(
            ts: i64,
            tu: TimeUnit,
            tz: Option<T>,
            grid: TruncationGrid,
            relative_to: Option<i64>,
        ) -> DaftResult<i64>
        where
            T: chrono::TimeZone,
        {
            let out_of_range = || {
                DaftError::ValueError(format!(
                    "Truncated timestamp is out of range for time unit {tu}"
                ))
            };
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
                    let truncated_naive_ts = grid
                        .truncate(naive_ts, naive_origin)
                        .ok_or_else(out_of_range)?;
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
                None => grid
                    .truncate(ts, relative_to.unwrap_or(0))
                    .ok_or_else(out_of_range),
            }
        }

        let mut builder = arrow::array::Int64Builder::with_capacity(physical.len());
        for ts in physical {
            match ts {
                None => builder.append_null(),
                Some(ts) => {
                    let truncated_ts = match tz_parsed {
                        Some(ParsedTimezone::Fixed(offset)) => {
                            truncate_single_ts(ts, *timeunit, Some(offset), grid, relative_to)
                        }
                        Some(ParsedTimezone::Tz(tz)) => {
                            truncate_single_ts(ts, *timeunit, Some(tz), grid, relative_to)
                        }
                        None => truncate_single_ts(
                            ts,
                            *timeunit,
                            None::<chrono_tz::Tz>,
                            grid,
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

    use super::{TruncationGrid, process_interval};
    use crate::prelude::TimeUnit;

    #[test]
    fn test_process_interval() -> DaftResult<()> {
        assert_eq!(process_interval("1 nanosecond")?, 1);
        assert_eq!(process_interval("2500 nanoseconds")?, 2500);
        assert_eq!(process_interval("1 week")?, 604_800_000_000_000);
        // Large counts of coarse units must not overflow or panic.
        assert_eq!(
            process_interval("9223372036854775807 weeks")?,
            i128::from(i64::MAX) * 604_800_000_000_000
        );
        for interval in ["0 days", "-1 days", "1 year", "1"] {
            assert!(process_interval(interval).is_err(), "{interval}");
        }
        Ok(())
    }

    #[test]
    fn test_truncation_grid_units_matches_nanos() {
        // The `i64` path for whole multiples of the unit must agree with exact nanosecond math.
        for (unit, tu) in [
            (1_000_000_000, TimeUnit::Seconds),
            (1_000, TimeUnit::Microseconds),
        ] {
            for units in [2, 60, 86_400, 604_800] {
                let interval = units * unit;
                let grid = TruncationGrid::new(interval, tu);
                assert!(matches!(grid, TruncationGrid::Units(_)));
                let nanos = TruncationGrid::Nanos { interval, unit };
                let bound = i64::MAX / unit as i64;
                for origin in [0, 7, -13] {
                    for ts in (-1_000..1_000).chain([bound, -bound]) {
                        assert_eq!(
                            grid.truncate(ts, origin),
                            nanos.truncate(ts, origin),
                            "ts={ts} origin={origin} interval={interval}"
                        );
                    }
                }
            }
        }
        // `ts - origin` overflowing `i64` falls back to `i128` instead of failing.
        let (ts, origin) = (i64::MAX - 5, -10);
        let expected = i128::from(ts) - (i128::from(ts) - i128::from(origin)).rem_euclid(60);
        assert_eq!(
            TruncationGrid::Units(60).truncate(ts, origin),
            Some(i64::try_from(expected).unwrap())
        );
        // A result outside the `i64` range is reported, not a panic.
        let weeks = i128::from(i64::MAX) * 604_800_000_000_000;
        assert_eq!(
            TruncationGrid::new(weeks, TimeUnit::Nanoseconds).truncate(-1, 0),
            None
        );
    }
}
