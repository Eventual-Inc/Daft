use crate::{
    datatypes::{
        logical::{DateArray, TimeArray, TimestampArray},
        DaftArrayType, Field, Int32Array, Int64Array, TimeUnit, UInt32Array,
    },
    DataType,
};
use arrow2::compute::arithmetics::ArraySub;
use chrono::{Duration, DurationRound, NaiveDate, NaiveTime, RoundingError, Timelike};
use common_error::{DaftError, DaftResult};

use super::as_arrow::AsArrow;

fn process_interval(interval: &str) -> DaftResult<Duration> {
    let (count_str, unit) = interval
        .split_once(' ')
        .ok_or_else(|| DaftError::ValueError(format!("Invalid interval string: {interval}")))?;

    let count = count_str
        .parse::<i64>()
        .map_err(|e| DaftError::ValueError(format!("Invalid interval count: {e}")))?;

    match unit {
        "week" => Ok(Duration::weeks(count)),
        "day" => Ok(Duration::days(count)),
        "hour" => Ok(Duration::hours(count)),
        "minute" => Ok(Duration::minutes(count)),
        "second" => Ok(Duration::seconds(count)),
        "millisecond" => Ok(Duration::milliseconds(count)),
        "microsecond" => Ok(Duration::microseconds(count)),
        "nanosecond" => Ok(Duration::nanoseconds(count)),
        _ => Err(DaftError::ValueError(format!(
            "Invalid interval unit: {unit}"
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
            Some(tz) => match arrow2::temporal_conversions::parse_offset(tz) {
                Ok(tz) => Ok(arrow2::array::PrimitiveArray::<i64>::from_iter(
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
                )),
                Err(e) => Err(DaftError::TypeError(format!(
                    "Cannot parse timezone in Timestamp datatype: {}, error: {}",
                    tz, e
                ))),
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

    pub fn hour(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        let date_arrow = match tz {
            Some(tz) => match arrow2::temporal_conversions::parse_offset(tz) {
                Ok(tz) => Ok(arrow2::array::UInt32Array::from_iter(physical.iter().map(
                    |ts| {
                        ts.map(|ts| {
                            arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz).hour()
                        })
                    },
                ))),
                Err(e) => Err(DaftError::TypeError(format!(
                    "Cannot parse timezone in Timestamp datatype: {}, error: {}",
                    tz, e
                ))),
            },
            None => Ok(arrow2::array::UInt32Array::from_iter(physical.iter().map(
                |ts| {
                    ts.map(|ts| {
                        arrow2::temporal_conversions::timestamp_to_naive_datetime(*ts, tu).hour()
                    })
                },
            ))),
        }?;

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(date_arrow),
        )
    }

    pub fn truncate(&self, interval: &str, start_time: &Option<i64>) -> DaftResult<Self> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        let duration = process_interval(interval)?;

        macro_rules! datetime_to_timestamp {
            ($dt:expr) => {
                match tu {
                    arrow2::datatypes::TimeUnit::Second => $dt.timestamp(),
                    arrow2::datatypes::TimeUnit::Millisecond => $dt.timestamp_millis(),
                    arrow2::datatypes::TimeUnit::Microsecond => $dt.timestamp_micros(),
                    arrow2::datatypes::TimeUnit::Nanosecond => {
                        $dt.timestamp_nanos_opt()
                            .ok_or(DaftError::ValueError(format!(
                                "Error truncating timestamp, result is out of range : {{$dt}}"
                            )))?
                    }
                }
            };
        }

        let truncate_ts = |ts: i64| -> DaftResult<i64> {
            let ts = match start_time {
                Some(st) => {
                    if *st > ts {
                        return Err(DaftError::ValueError(format!(
                            "Start time is greater than timestamp: {st} > {ts}"
                        )));
                    }
                    ts - st
                }
                None => ts,
            };

            let truncated_ts = match tz {
                Some(tz) => {
                    if let Ok(tz) = arrow2::temporal_conversions::parse_offset(tz) {
                        let dt = arrow2::temporal_conversions::timestamp_to_datetime(ts, tu, &tz);
                        match dt.duration_trunc(duration) {
                            Ok(dt) => Ok(datetime_to_timestamp!(dt)),
                            Err(RoundingError::DurationExceedsTimestamp) => Ok(0),
                            Err(e) => Err(DaftError::ValueError(format!(
                                "Error truncating timestamp: {e}"
                            ))),
                        }
                    } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(tz) {
                        let dt = arrow2::temporal_conversions::timestamp_to_datetime(ts, tu, &tz);
                        match dt.duration_trunc(duration) {
                            Ok(dt) => Ok(datetime_to_timestamp!(dt)),
                            Err(RoundingError::DurationExceedsTimestamp) => Ok(0),
                            Err(e) => Err(DaftError::ValueError(format!(
                                "Error truncating timestamp: {e}"
                            ))),
                        }
                    } else {
                        Err(DaftError::TypeError(format!(
                            "Cannot parse timezone in Timestamp datatype: {}",
                            tz
                        )))
                    }
                }
                None => {
                    let dt = arrow2::temporal_conversions::timestamp_to_naive_datetime(ts, tu);
                    match dt.duration_trunc(duration) {
                        Ok(dt) => Ok(datetime_to_timestamp!(dt)),
                        Err(RoundingError::DurationExceedsTimestamp) => Ok(0),
                        Err(e) => Err(DaftError::ValueError(format!(
                            "Error truncating timestamp: {e}"
                        ))),
                    }
                }
            };
            match start_time {
                Some(st) => truncated_ts.map(|ts| ts + st),
                None => truncated_ts,
            }
        };

        let result_timestamps = physical
            .iter()
            .map(|ts| ts.map_or(Ok(None), |ts| truncate_ts(*ts).map(Some)))
            .collect::<DaftResult<arrow2::array::PrimitiveArray<i64>>>()?;

        Ok(TimestampArray::new(
            Field::new(self.name(), self.data_type().clone()),
            Int64Array::from((self.name(), Box::new(result_timestamps))),
        ))
    }
}
