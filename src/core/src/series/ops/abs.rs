use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;
impl Series {
    pub fn abs(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;

        use DataType::*;
        match self.data_type() {
            Int8 => Ok(self.i8().unwrap().abs()?.into_series()),
            Int16 => Ok(self.i16().unwrap().abs()?.into_series()),
            Int32 => Ok(self.i32().unwrap().abs()?.into_series()),
            Int64 => Ok(self.i64().unwrap().abs()?.into_series()),
            UInt8 | UInt16 | UInt32 | UInt64 => Ok(self.clone()),
            Float32 => Ok(self.f32().unwrap().abs()?.into_series()),
            Float64 => Ok(self.f64().unwrap().abs()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "abs not implemented for {}",
                dt
            ))),
        }
    }
}
