use crate::array::BaseArray;
use crate::{
    datatypes::{DataType, DateType},
    error::DaftError,
    error::DaftResult,
    series::Series,
};

impl Series {
    pub fn dt_year(&self) -> DaftResult<Self> {
        if !matches!(self.data_type(), DataType::Date) {
            return Err(DaftError::ComputeError(format!(
                "Can only run year() operation on DateType, got {}",
                self.data_type()
            )));
        }

        let downcasted = self.downcast::<DateType>()?;
        Ok(downcasted.year()?.into_series())
    }
}
