use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{array_impl::IntoSeries, Series},
};

impl Series {
    pub fn exp(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Float32 => Ok(self.f32().unwrap().exp()?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().exp()?.into_series()),
            dt if dt.is_integer() => self.cast(&DataType::Float64).unwrap().exp(),
            dt => Err(DaftError::TypeError(format!(
                "exp not implemented for {}",
                dt
            ))),
        }
    }
}
