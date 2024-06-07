use crate::{
    datatypes::{
        logical::{DateArray, TimeArray, TimestampArray},
        DaftArrayType, Field, Int32Array, Int64Array, TimeUnit, UInt32Array,
    },
    DataType,
};
use arrow2::{array::PrimitiveArray, compute::arithmetics::ArraySub};
use chrono::{Duration, NaiveDate, NaiveTime, Timelike};
use common_error::{DaftError, DaftResult};

use super::as_arrow::AsArrow;

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
        _ => return Err(DaftError::ValueError(format!(
            "Invalid interval unit: {unit}. Expected one of: week, day, hour, minute, second, millisecond, microsecond, nanosecond"
        ))),
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
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let day_arr = arrow2::compute::temporal::day(&input_array)?;
        Ok((self.name(), Box::new(day_arr)).into())
    }

    pub fn month(&self) -> DaftResult<UInt32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let month_arr = arrow2::compute::temporal::month(&input_array)?;
        Ok((self.name(), Box::new(month_arr)).into())
    }

    pub fn year(&self) -> DaftResult<Int32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let year_arr = arrow2::compute::temporal::year(&input_array)?;
        Ok((self.name(), Box::new(year_arr)).into())
    }

    pub fn day_of_week(&self) -> DaftResult<UInt32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let day_arr = arrow2::compute::temporal::weekday(&input_array)?;
        Ok((self.name(), Box::new(day_arr.sub(&1))).into())
    }
}

impl TimestampArray {
    pub fn date(&self) -> DaftResult<DateArray> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date_arrow = match tz {
            Some(tz) => {
                if let Ok(tz) = arrow2::temporal_conversions::parse_offset(tz) {
                    Ok(arrow2::array::PrimitiveArray::<i32>::from_iter(
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                (arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz)
                                    .date_naive()
                                    - epoch_date)
                                    .num_days() as i32
                            })
                        }),
                    ))
                } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(tz) {
                    Ok(arrow2::array::PrimitiveArray::<i32>::from_iter(
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                (arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz)
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
            None => Ok(arrow2::array::PrimitiveArray::<i32>::from_iter(
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        (arrow2::temporal_conversions::timestamp_to_naive_datetime(*ts, tu).date()
                            - epoch_date)
                            .num_days() as i32
                    })
                }),
            )),
        }?;
        Ok(DateArray::new(
            Field::new(self.name(), DataType::Date),
            Int32Array::from((self.name(), Box::new(date_arrow))),
        ))
    }

    pub fn time(&self, timeunit_for_cast: &TimeUnit) -> DaftResult<TimeArray> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        if !matches!(
            timeunit_for_cast,
            TimeUnit::Microseconds | TimeUnit::Nanoseconds
        ) {
            return Err(DaftError::ValueError(format!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}")));
        }
        let time_arrow = match tz {
            Some(tz) => {
                if let Ok(tz) = arrow2::temporal_conversions::parse_offset(tz) {
                Ok(arrow2::array::PrimitiveArray::<i64>::from_iter(
                    physical.iter().map(|ts| {
                        ts.map(|ts| {
                            let dt =
                                arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz);
                                let time_delta = dt.time() - NaiveTime::from_hms_opt(0,0,0).unwrap();
                                match timeunit_for_cast {
                                    TimeUnit::Microseconds => time_delta.num_microseconds().unwrap(),
                                    TimeUnit::Nanoseconds => time_delta.num_nanoseconds().unwrap(),
                                    _ => unreachable!("Only microseconds and nanoseconds time units are supported for the Time dtype, but got {timeunit_for_cast}"),
                                }
                            })
                        }),
                    ))
                } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(tz) {
                Ok(arrow2::array::PrimitiveArray::<i64>::from_iter(
                    physical.iter().map(|ts| {
                        ts.map(|ts| {
                            let dt =
                                arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz);
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
                )))}
            },
            None => Ok(arrow2::array::PrimitiveArray::<i64>::from_iter(
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        let dt = arrow2::temporal_conversions::timestamp_to_naive_datetime(*ts, tu);
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
            Int64Array::from((self.name(), Box::new(time_arrow))),
        ))
    }

    pub fn truncate(&self, interval: &str, relative_to: &Option<i64>) -> DaftResult<Self> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let duration = process_interval(interval, *timeunit)?;

        fn truncate_single_ts<T>(
            ts: i64,
            tu: TimeUnit,
            tz: Option<T>,
            duration: i64,
            relative_to: &Option<i64>,
        ) -> DaftResult<i64>
        where
            T: chrono::TimeZone,
        {
            // To truncate a timestamp, we need to subtract off the truncate_by amount, which is essentially the amount of time past the last interval boundary.
            // We can calculate this by taking the modulo of the timestamp with the duration.
            match tz {
                Some(tz) => {
                    let tu_arrow = tu.to_arrow();
                    let original_dt =
                        arrow2::temporal_conversions::timestamp_to_datetime(ts, tu_arrow, &tz);
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
                            let rt_dt = arrow2::temporal_conversions::timestamp_to_datetime(
                                *rt, tu_arrow, &tz,
                            );
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

        let result_timestamps = physical
            .iter()
            .map(|ts| {
                ts.map_or(Ok(None), |ts| {
                    let truncated_ts = match tz {
                        Some(tz) => {
                            if let Ok(tz) = arrow2::temporal_conversions::parse_offset(tz) {
                                truncate_single_ts(*ts, *timeunit, Some(tz), duration, relative_to)
                            } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(tz)
                            {
                                truncate_single_ts(*ts, *timeunit, Some(tz), duration, relative_to)
                            } else {
                                Err(DaftError::TypeError(format!(
                                    "Cannot parse timezone in Timestamp datatype: {}",
                                    tz
                                )))
                            }
                        }
                        None => truncate_single_ts(
                            *ts,
                            *timeunit,
                            None::<chrono_tz::Tz>,
                            duration,
                            relative_to,
                        ),
                    };
                    truncated_ts.map(Some)
                })
            })
            .collect::<DaftResult<arrow2::array::PrimitiveArray<i64>>>()?;

        Ok(TimestampArray::new(
            Field::new(self.name(), self.data_type().clone()),
            Int64Array::from((self.name(), Box::new(result_timestamps))),
        ))
    }
}

impl TimeArray {
    pub fn hour(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow();
        let tu = match self.data_type() {
            DataType::Time(time_unit) => time_unit.to_arrow(),
            _ => unreachable!("TimeArray must have Time datatype"),
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time =
                        arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &chrono::Utc)
                            .time();
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
        let physical = self.physical.as_arrow();
        let tu = match self.data_type() {
            DataType::Time(time_unit) => time_unit.to_arrow(),
            _ => unreachable!("TimeArray must have Time datatype"),
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time =
                        arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &chrono::Utc)
                            .time();
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
        let physical = self.physical.as_arrow();
        let tu = match self.data_type() {
            DataType::Time(time_unit) => time_unit.to_arrow(),
            _ => unreachable!("TimeArray must have Time datatype"),
        };

        let date_arrow = physical
            .iter()
            .map(|ts| {
                ts.map(|ts| {
                    let naive_time =
                        arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &chrono::Utc)
                            .time();
                    naive_time.second() as u32
                })
            })
            .collect::<Vec<_>>();

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(PrimitiveArray::from(date_arrow)),
        )
    }
}
