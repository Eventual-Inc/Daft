use crate::datatypes::TimeUnit;
use crate::series::array_impl::IntoSeries;
use crate::{datatypes::DataType, series::Series};

use common_error::{DaftError, DaftResult};

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

    pub fn dt_truncate(&self, interval: &str, relative_to: &Self) -> DaftResult<Self> {
        match (self.data_type(), relative_to.data_type()) {
            (DataType::Timestamp(self_tu,self_tz), DataType::Timestamp(start_tu,start_tz)) if self_tu == start_tu && self_tz == start_tz => {
                let ts_array = self.timestamp()?;
                let relative_to = match relative_to.len() {
                    1 => relative_to.timestamp()?.get(0),
                    _ => {
                        return Err(DaftError::ComputeError(format!(
                            "Expected 1 item for relative_to, got {}",
                            relative_to.len()
                        )))
                    }
                };
                Ok(ts_array.truncate(interval, &relative_to)?.into_series())
            }
            (DataType::Timestamp(..), DataType::Timestamp(..)) => Err(DaftError::ComputeError(format!(
                "Can only run truncate() operation if self and relative_to have the same timeunit and timezone, got {} {}",
                self.data_type(),
                relative_to.data_type()
            ))),
            (DataType::Timestamp(..), DataType::Null) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.truncate(interval, &None)?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run truncate() operation on temporal types, got {} {}",
                self.data_type(),
                relative_to.data_type()
            ))),
        }
    }
}
