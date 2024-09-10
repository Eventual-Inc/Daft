use common_error::DaftError;
use common_error::DaftResult;

use crate::datatypes::DataType;
use crate::series::array_impl::IntoSeries;
use crate::series::Series;

impl Series {
    pub fn exp(&self) -> DaftResult<Series> {
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
