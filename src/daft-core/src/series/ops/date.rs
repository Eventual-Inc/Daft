use crate::series::array_impl::IntoSeries;
use crate::{
    datatypes::{logical::DateArray, DataType},
    series::Series,
};
use common_error::{DaftError, DaftResult};

impl Series {
    pub fn dt_day(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.downcast::<DateArray>()?;
                Ok(downcasted.day()?.into_series())
            }
            DataType::Timestamp(..) => {
                let date_series = self.cast(&DataType::Date)?;
                date_series.dt_day()
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run day() operation on DateType, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_month(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.downcast::<DateArray>()?;
                Ok(downcasted.month()?.into_series())
            }
            DataType::Timestamp(..) => {
                let date_series = self.cast(&DataType::Date)?;
                date_series.dt_month()
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run month() operation on DateType, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_year(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Date => {
                let downcasted = self.downcast::<DateArray>()?;
                Ok(downcasted.year()?.into_series())
            }
            DataType::Timestamp(..) => {
                let date_series = self.cast(&DataType::Date)?;
                date_series.dt_year()
            }
            _ => Err(DaftError::ComputeError(format!(
                "Can only run year() operation on DateType, got {}",
                self.data_type()
            ))),
        }
    }

    pub fn dt_day_of_week(&self) -> DaftResult<Self> {
        if !matches!(self.data_type(), DataType::Date) {
            return Err(DaftError::ComputeError(format!(
                "Can only run day_of_week() operation on DateType, got {}",
                self.data_type()
            )));
        }

        let downcasted = self.downcast::<DateArray>()?;
        Ok(downcasted.day_of_week()?.into_series())
    }
}
