use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn cbrt(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 => self.cast(&Float32).unwrap().cbrt(),
            Int16 => self.cast(&Float32).unwrap().cbrt(),
            UInt8 => self.cast(&Float32).unwrap().cbrt(),
            UInt16 => self.cast(&Float32).unwrap().cbrt(),
            Int32 => self.cast(&Float64).unwrap().cbrt(),
            Int64 => self.cast(&Float64).unwrap().cbrt(),
            UInt32 => self.cast(&Float64).unwrap().cbrt(),
            UInt64 => self.cast(&Float64).unwrap().cbrt(),
            Float32 => Ok(self.f32().unwrap().cbrt()?.into_series()),
            Float64 => Ok(self.f64().unwrap().cbrt()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "cbrt not implemented for {}",
                dt
            ))),
        }
    }
}
