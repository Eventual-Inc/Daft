use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    compute::{DatePart, date_part},
    datatypes::IntervalMonthDayNano,
    error::ArrowError,
};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use common_error::{DaftError, DaftResult};

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
        let physical = self.physical.as_arrow()?;
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = *timeunit;
        let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let field = Field::new(self.name(), DataType::Int32);
        let date_physical = match tz {
            Some(tz) => {
                if let Ok(tz) = daft_schema::time_unit::parse_offset(tz) {
                    Ok(DataArray::<Int32Type>::from_iter(
                        field,
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                (daft_schema::time_unit::timestamp_to_datetime(ts, tu, &tz)
                                    .date_naive()
                                    - epoch_date)
                                    .num_days() as i32
                            })
                        }),
                    ))
                } else if let Ok(tz) = daft_schema::time_unit::parse_offset_tz(tz) {
                    Ok(DataArray::<Int32Type>::from_iter(
                        field,
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                (daft_schema::time_unit::timestamp_to_datetime(ts, tu, &tz)
                                    .date_naive()
                                    - epoch_date)
                                    .num_days() as i32
                            })
                        }),
                    ))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Cannot parse timezone in Timestamp datatype: {}",
                        tz
                    )))
                }
            }
            None => Ok(DataArray::<Int32Type>::from_iter(
                field,
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        (daft_schema::time_unit::timestamp_to_naive_datetime(ts, tu).date()
                            - epoch_date)
                            .num_days() as i32
                    })
                }),
            )),
        }?;
        Ok(DateArray::new(
            Field::new(self.name(), DataType::Date),
            date_physical,
        ))
    }

    pub fn time(&self, timeunit_for_cast: &TimeUnit) -> DaftResult<TimeArray> {
        let physical = self.physical.as_arrow()?;
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
        let field = Field::new(self.name(), DataType::Int64);
        let time_physical = match tz {
            Some(tz) => {
                if let Ok(tz) = daft_schema::time_unit::parse_offset(tz) {
                    Ok(DataArray::<Int64Type>::from_iter(
                        field,
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                let dt = daft_schema::time_unit::timestamp_to_datetime(ts, tu, &tz);
                                let time_delta = dt.time() - NaiveTime::from_hms_opt(0,0,0).unwrap();
                                match timeunit_for_cast {
                                    TimeUnit::Microseconds => time_delta.num_microseconds().unwrap(),
                                    TimeUnit::Nanoseconds => time_delta.num_nanoseconds().unwrap(),
                                    _ => unreachable!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"),
                                }
                            })
                        }),
                    ))
                } else if let Ok(tz) = daft_schema::time_unit::parse_offset_tz(tz) {
                    Ok(DataArray::<Int64Type>::from_iter(
                        field,
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                let dt = daft_schema::time_unit::timestamp_to_datetime(ts, tu, &tz);
                                let time_delta = dt.time() - NaiveTime::from_hms_opt(0,0,0).unwrap();
                                match timeunit_for_cast {
                                    TimeUnit::Microseconds => time_delta.num_microseconds().unwrap(),
                                    TimeUnit::Nanoseconds => time_delta.num_nanoseconds().unwrap(),
                                    _ => unreachable!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"),
                                }
                            })
                        }),
                    ))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Cannot parse timezone in Timestamp datatype: {}",
                        tz
                    )))
                }
            },
            None => Ok(DataArray::<Int64Type>::from_iter(
                field,
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        let dt = daft_schema::time_unit::timestamp_to_naive_datetime(ts, tu);
                        let time_delta = dt.time() - NaiveTime::from_hms_opt(0,0,0).unwrap();
                        match timeunit_for_cast {
                            TimeUnit::Microseconds => time_delta.num_microseconds().unwrap(),
                            TimeUnit::Nanoseconds => time_delta.num_nanoseconds().unwrap(),
                            _ => unreachable!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"),
                        }
                    })
                }),
            )),
        }?;
        Ok(TimeArray::new(
            Field::new(self.name(), DataType::Time(*timeunit_for_cast)),
            time_physical,
        ))
    }

    pub fn truncate(&self, interval: &str, relative_to: Option<i64>) -> DaftResult<Self> {
        let physical = self.physical.as_arrow()?;
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let duration = process_interval(interval, *timeunit)?;

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
                    let original_dt = daft_schema::time_unit::timestamp_to_datetime(ts, tu, &tz);
                    let naive_ts = match tu {
                        TimeUnit::Seconds => original_dt.naive_local().and_utc().timestamp(),
                        TimeUnit::Milliseconds => {
                            original_dt.naive_local().and_utc().timestamp_millis()
                        }
                        TimeUnit::Microseconds => {
                            original_dt.naive_local().and_utc().timestamp_micros()
                        }
                        TimeUnit::Nanoseconds => original_dt
                            .naive_local()
                            .and_utc()
                            .timestamp_nanos_opt()
                            .ok_or(DaftError::ValueError(format!(
                                "Error truncating timestamp {ts} in nanosecond units"
                            )))?,
                    };

                    let mut truncate_by_amount = match relative_to {
                        Some(rt) => {
                            let rt_dt = daft_schema::time_unit::timestamp_to_datetime(rt, tu, &tz);
                            let naive_rt_ts = match tu {
                                TimeUnit::Seconds => rt_dt.naive_local().and_utc().timestamp(),
                                TimeUnit::Milliseconds => {
                                    rt_dt.naive_local().and_utc().timestamp_millis()
                                }
                                TimeUnit::Microseconds => {
                                    rt_dt.naive_local().and_utc().timestamp_micros()
                                }
                                TimeUnit::Nanoseconds => {
                                    rt_dt.naive_local().and_utc().timestamp_nanos_opt().ok_or(
                                        DaftError::ValueError(format!(
                                            "Error truncating timestamp {ts} in nanosecond units"
                                        )),
                                    )?
                                }
                            };
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
                    match tu {
                        TimeUnit::Seconds => Ok(truncated_dt.timestamp()),
                        TimeUnit::Milliseconds => Ok(truncated_dt.timestamp_millis()),
                        TimeUnit::Microseconds => Ok(truncated_dt.timestamp_micros()),
                        TimeUnit::Nanoseconds => {
                            truncated_dt
                                .timestamp_nanos_opt()
                                .ok_or(DaftError::ValueError(format!(
                                    "Error truncating timestamp {ts} in nanosecond units"
                                )))
                        }
                    }
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
                    let truncated_ts = match tz {
                        Some(tz) => {
                            if let Ok(tz) = daft_schema::time_unit::parse_offset(tz) {
                                truncate_single_ts(ts, *timeunit, Some(tz), duration, relative_to)
                            } else if let Ok(tz) = daft_schema::time_unit::parse_offset_tz(tz) {
                                truncate_single_ts(ts, *timeunit, Some(tz), duration, relative_to)
                            } else {
                                Err(DaftError::TypeError(format!(
                                    "Cannot parse timezone in Timestamp datatype: {}",
                                    tz
                                )))
                            }
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
        let result_i64 = arrow::compute::cast(result.as_ref(), &arrow::datatypes::DataType::Int64)?;
        let physical_field = Arc::new(Field::new(self.name(), DataType::Int64));
        Ok(Self::new(
            self.field.clone(),
            Int64Array::from_arrow(physical_field, result_i64)?,
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
                let datetime = daft_schema::time_unit::timestamp_to_datetime(ts, *tu, &chrono::Utc);
                datetime
                    .date_naive()
                    .signed_duration_since(UNIX_EPOCH_DATE)
                    .num_days() as u64
            })
        });

        Ok(DataArray::<UInt64Type>::from_iter(
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
