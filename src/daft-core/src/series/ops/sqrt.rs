use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn sqrt(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 => self.cast(&Float32).unwrap().sqrt(),
            Int16 => self.cast(&Float32).unwrap().sqrt(),
            UInt8 => self.cast(&Float32).unwrap().sqrt(),
            UInt16 => self.cast(&Float32).unwrap().sqrt(),
            Int32 => self.cast(&Float64).unwrap().sqrt(),
            Int64 => self.cast(&Float64).unwrap().sqrt(),
            UInt32 => self.cast(&Float64).unwrap().sqrt(),
            UInt64 => self.cast(&Float64).unwrap().sqrt(),
            Float32 => Ok(self.f32().unwrap().sqrt()?.into_series()),
            Float64 => Ok(self.f64().unwrap().sqrt()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "sqrt not implemented for {}",
                dt
            ))),
        }
    }
}
