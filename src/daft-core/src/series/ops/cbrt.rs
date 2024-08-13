use common_error::DaftError;
use common_error::DaftResult;

use crate::datatypes::DataType;
use crate::series::array_impl::IntoSeries;
use crate::series::Series;

impl Series {
    pub fn cbrt(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => {
                self.cast(&DataType::Float32).unwrap().cbrt()
            }
            DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64 => {
                self.cast(&DataType::Float64).unwrap().cbrt()
            }
            DataType::Float32 => Ok(self.f32().unwrap().cbrt()?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().cbrt()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "cbrt not implemented for {}",
                dt
            ))),
        }
    }
}
