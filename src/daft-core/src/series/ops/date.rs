use crate::array::ops::as_arrow::AsArrow;
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
            DataType::Timestamp(..) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.hour()?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run day() operation on temporal types, got {}",
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

    pub fn dt_truncate(&self, interval: &str, start_time: &Self) -> DaftResult<Self> {
        match (self.data_type(), start_time.data_type()) {
            (DataType::Timestamp(..), DataType::Timestamp(..)) => {
                let ts_array = self.timestamp()?;
                let start_time = start_time.timestamp()?.physical.as_arrow().get(0);
                Ok(ts_array.truncate(interval, &start_time)?.into_series())
            }
            (DataType::Timestamp(..), DataType::Null) => {
                let ts_array = self.timestamp()?;
                Ok(ts_array.truncate(interval, &None)?.into_series())
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run truncate() operation on temporal types, got {} {}",
                self.data_type(),
                start_time.data_type()
            ))),
        }
    }
}
