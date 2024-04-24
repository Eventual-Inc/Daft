use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn sqrt(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 => Ok(self.i8().unwrap().sqrt()?.into_series()),
            Int16 => Ok(self.i16().unwrap().sqrt()?.into_series()),
            UInt8 => Ok(self.u8().unwrap().sqrt()?.into_series()),
            UInt16 => Ok(self.u16().unwrap().sqrt()?.into_series()),
            Int32 => Ok(self.i32().unwrap().sqrt()?.into_series()),
            Int64 => Ok(self.i64().unwrap().sqrt()?.into_series()),
            UInt32 => Ok(self.u32().unwrap().sqrt()?.into_series()),
            UInt64 => Ok(self.u64().unwrap().sqrt()?.into_series()),
            Float32 => Ok(self.f32().unwrap().sqrt()?.into_series()),
            Float64 => Ok(self.f64().unwrap().sqrt()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "sqrt not implemented for {}",
                dt
            ))),
        }
    }
}
