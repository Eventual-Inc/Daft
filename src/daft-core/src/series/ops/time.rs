use arrow::util::display::FormatOptions;
use common_error::{DaftError, DaftResult};
use daft_schema::field::Field;

use crate::{
    datatypes::{DataType, TimeUnit},
    series::{Series, array_impl::IntoSeries},
};

impl Series {
    pub fn dt_date(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => Ok(self.clone()),
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.date()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run date() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_day(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.date()?;
                Ok(downcasted.day()?.into_series())
            }
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.date()?.day()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run day() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_hour(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.hour()?.into_series())
            }
            DataType::Time(_) => {
                let time_array = self.time()?;
                Ok(time_array.hour()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run hour() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_minute(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.minute()?.into_series())
            }
            DataType::Time(_) => {
                let time_array = self.time()?;
                Ok(time_array.minute()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run minute() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_second(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.second()?.into_series())
            }
            DataType::Time(_) => {
                let time_array = self.time()?;
                Ok(time_array.second()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run second() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_millisecond(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.millisecond()?.into_series())
            }
            DataType::Time(_) => {
                let time_array = self.time()?;
                Ok(time_array.millisecond()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run millisecond() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_microsecond(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.microsecond()?.into_series())
            }
            DataType::Time(_) => {
                let time_array = self.time()?;
                Ok(time_array.microsecond()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run microsecond() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_nanosecond(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.nanosecond()?.into_series())
            }
            DataType::Time(_) => {
                let time_array = self.time()?;
                Ok(time_array.nanosecond()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run nanosecond() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_unix_date(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(_, _) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.unix_date()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run unix_date() operation on timestamp types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_time(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                let ts_array = self.timestamp()?;
                Ok(ts_array.time(&tu)?.into_series())
            }
            DataType::Time(_) => Ok(self.clone()),
            _ => Err(DaftError::ComputeError(format!(
                "Can only run time() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_month(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.date()?;
                Ok(downcasted.month()?.into_series())
            }
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.date()?.month()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run month() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_quarter(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.date()?;
                Ok(downcasted.quarter()?.into_series())
            }
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.date()?.quarter()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run quarter() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_year(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.date()?;
                Ok(downcasted.year()?.into_series())
            }
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.date()?.year()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run year() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_day_of_week(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.date()?;
                Ok(downcasted.day_of_week()?.into_series())
            }
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.date()?.day_of_week()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run dt_day_of_week() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_day_of_month(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(_, _) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.day_of_month()?.into_series())
            }
            DataType::Date => {
                let date_array = self.date()?;
                Ok(date_array.day_of_month()?.into_series())
            }

            _ => Err(DaftError::ComputeError(format!(
                "Can only run day_of_month() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_day_of_year(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(_, _) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.day_of_year()?.into_series())
            }
            DataType::Date => {
                let date_array = self.date()?;
                Ok(date_array.day_of_year()?.into_series())
            }

            _ => Err(DaftError::ComputeError(format!(
                "Can only run day_of_year() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_week_of_year(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(_, _) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.week_of_year()?.into_series())
            }
            DataType::Date => {
                let date_array = self.date()?;
                Ok(date_array.week_of_year()?.into_series())
            }

            _ => Err(DaftError::ComputeError(format!(
                "Can only run week_of_year() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_truncate(&self, interval: &str, relative_to: &Self) -> DaftResult<Self> {
        match (self.data_type(), relative_to.data_type()) {
            (DataType::Timestamp(self_tu, self_tz), DataType::Timestamp(start_tu, start_tz))
                if self_tu == start_tu && self_tz == start_tz =>
            {
                let ts_array = self.timestamp()?;
                let relative_to = match relative_to.len() {
                    1 => relative_to.timestamp()?.get(0),
                    _ => {
                        return Err(DaftError::ComputeError(format!(
                            "Expected 1 item for relative_to, got {}",
                            relative_to.len()
                        )));
                    }
                };
                Ok(ts_array.truncate(interval, relative_to)?.into_series())
            }
            (DataType::Timestamp(..), DataType::Timestamp(..)) => {
                Err(DaftError::ComputeError(format!(
                    "Can only run truncate() operation if self and relative_to have the same timeunit and timezone, got {} {}",
                    self.data_type(),
                    relative_to.data_type()
                )))
            }
            (DataType::Timestamp(..), DataType::Null) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.truncate(interval, None)?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run truncate() operation on temporal types, got {} {}",
                self.data_type(),
                relative_to.data_type()
            ))),
        }
    }

    pub fn dt_to_unix_epoch(&self, time_unit: TimeUnit) -> DaftResult<Self> {
        let cast_to = DataType::Timestamp(time_unit, None);
        self.cast(&cast_to)?.cast(&DataType::Int64)
    }

    pub fn dt_strftime(&self, format: Option<&str>) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Timestamp(..) | DataType::Date | DataType::Time(_) => {
                let arrow_arr = self.to_arrow()?;
                let cast_options = arrow::compute::CastOptions {
                    safe: true,
                    format_options: FormatOptions::new()
                        .with_date_format(format)
                        .with_datetime_format(format)
                        .with_timestamp_format(format)
                        .with_time_format(format),
                };

                let out = arrow::compute::cast_with_options(
                    arrow_arr.as_ref(),
                    &DataType::Utf8.to_arrow().unwrap(),
                    &cast_options,
                )?;

                let field = Field::new(self.name().to_string(), DataType::Utf8);
                Self::from_arrow(field, out)
            }

            _ => Err(DaftError::ComputeError(format!(
                "Can only run to_string() operation on temporal types, got {}",
                self.data_type()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_schema::field::Field;

    use crate::{
        datatypes::{DataType, Int32Array, Int64Array, TimeUnit},
        prelude::{DateArray, TimeArray, TimestampArray},
        series::IntoSeries,
    };

    #[test]
    fn strftime_timestamp_default_format() -> DaftResult<()> {
        // 2023-01-01T12:00:00 in seconds since epoch
        let ts_secs: Vec<i64> = vec![1672574400];
        let physical = Int64Array::from_slice("ts", &ts_secs);
        let ts = TimestampArray::new(
            Field::new("ts", DataType::Timestamp(TimeUnit::Seconds, None)),
            physical,
        );
        let series = ts.into_series();
        let result = series.dt_strftime(None)?;

        assert_eq!(*result.data_type(), DataType::Utf8);
        assert_eq!(result.len(), 1);
        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "2023-01-01T12:00:00");
        Ok(())
    }

    #[test]
    fn strftime_timestamp_custom_format() -> DaftResult<()> {
        // 2023-01-01T12:00:00 in seconds since epoch
        let ts_secs: Vec<i64> = vec![1672574400];
        let physical = Int64Array::from_slice("ts", &ts_secs);
        let ts = TimestampArray::new(
            Field::new("ts", DataType::Timestamp(TimeUnit::Seconds, None)),
            physical,
        );
        let series = ts.into_series();
        let result = series.dt_strftime(Some("%Y/%m/%d %H:%M:%S"))?;

        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "2023/01/01 12:00:00");
        Ok(())
    }

    #[test]
    fn strftime_timestamp_microseconds() -> DaftResult<()> {
        // 2023-01-01T12:00:00 in microseconds since epoch
        let ts_us: Vec<i64> = vec![1672574400_000_000];
        let physical = Int64Array::from_slice("ts", &ts_us);
        let ts = TimestampArray::new(
            Field::new("ts", DataType::Timestamp(TimeUnit::Microseconds, None)),
            physical,
        );
        let series = ts.into_series();
        let result = series.dt_strftime(None)?;

        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "2023-01-01T12:00:00");
        Ok(())
    }

    #[test]
    fn strftime_timestamp_multiple_values() -> DaftResult<()> {
        // 2023-01-01T00:00:00, 2023-01-02T00:00:00, 2023-01-03T00:00:00 in seconds
        let ts_secs: Vec<i64> = vec![1672531200, 1672617600, 1672704000];
        let physical = Int64Array::from_slice("ts", &ts_secs);
        let ts = TimestampArray::new(
            Field::new("ts", DataType::Timestamp(TimeUnit::Seconds, None)),
            physical,
        );
        let series = ts.into_series();
        let result = series.dt_strftime(Some("%Y-%m-%d"))?;

        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "2023-01-01");
        assert_eq!(arr.get(1).unwrap(), "2023-01-02");
        assert_eq!(arr.get(2).unwrap(), "2023-01-03");
        Ok(())
    }

    #[test]
    fn strftime_date_default_format() -> DaftResult<()> {
        // Days since epoch: 2023-01-01 = 19358
        let days: Vec<i32> = vec![19358];
        let physical = Int32Array::from_slice("d", &days);
        let date = DateArray::new(Field::new("d", DataType::Date), physical);
        let series = date.into_series();
        let result = series.dt_strftime(None)?;

        assert_eq!(*result.data_type(), DataType::Utf8);
        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "2023-01-01");
        Ok(())
    }

    #[test]
    fn strftime_date_custom_format() -> DaftResult<()> {
        // 2023-01-01, 2023-01-02, 2023-01-03
        let days: Vec<i32> = vec![19358, 19359, 19360];
        let physical = Int32Array::from_slice("d", &days);
        let date = DateArray::new(Field::new("d", DataType::Date), physical);
        let series = date.into_series();
        let result = series.dt_strftime(Some("%m/%d/%Y"))?;

        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "01/01/2023");
        assert_eq!(arr.get(1).unwrap(), "01/02/2023");
        assert_eq!(arr.get(2).unwrap(), "01/03/2023");
        Ok(())
    }

    #[test]
    fn strftime_time_custom_format() -> DaftResult<()> {
        // 01:02:03 = 3723 seconds = 3723 * 1_000_000 microseconds
        let time_us: Vec<i64> = vec![3_723_000_000];
        let physical = Int64Array::from_slice("t", &time_us);
        let time = TimeArray::new(
            Field::new("t", DataType::Time(TimeUnit::Microseconds)),
            physical,
        );
        let series = time.into_series();
        let result = series.dt_strftime(Some("%H:%M:%S"))?;

        assert_eq!(*result.data_type(), DataType::Utf8);
        let arr = result.utf8()?;
        assert_eq!(arr.get(0).unwrap(), "01:02:03");
        Ok(())
    }

    #[test]
    fn strftime_invalid_type_returns_error() {
        let a = Int64Array::from_slice("a", &[1, 2, 3]);
        let series = a.into_series();
        let result = series.dt_strftime(None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Can only run to_string() operation on temporal types")
        );
    }
}
