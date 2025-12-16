use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{Series, array_impl::IntoSeries},
};

impl Series {
    pub fn pow(&self, exp: f64) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                let s = self.cast(&DataType::Float64)?;
                Ok(s.f64()?.pow(exp)?.into_series())
            }
            DataType::Float32 => Ok(self.f32()?.pow(exp as f32)?.into_series()),
            DataType::Float64 => Ok(self.f64()?.pow(exp)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "pow not implemented for {}",
                dt
            ))),
        }
    }
}
