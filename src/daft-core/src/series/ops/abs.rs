use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{array_impl::IntoSeries, Series},
};

impl Series {
    pub fn abs(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8 => Ok(self.i8().unwrap().abs()?.into_series()),
            DataType::Int16 => Ok(self.i16().unwrap().abs()?.into_series()),
            DataType::Int32 => Ok(self.i32().unwrap().abs()?.into_series()),
            DataType::Int64 => Ok(self.i64().unwrap().abs()?.into_series()),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(self.clone())
            }
            DataType::Float32 => Ok(self.f32().unwrap().abs()?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().abs()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "abs not implemented for {}",
                dt
            ))),
        }
    }
}
