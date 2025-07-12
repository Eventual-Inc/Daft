use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{array_impl::IntoSeries, Series},
};

impl Series {
    pub fn floor(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(self.clone()),
            DataType::Float32 => Ok(self.f32().unwrap().floor()?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().floor()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "floor not implemented for {}",
                dt
            ))),
        }
    }

    pub fn pow(&self, exp: f64) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(self.cast(&DataType::Float64)?.pow(exp)?),
            DataType::Float32 => Ok(self.f32().unwrap().pow(exp)?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().pow(exp)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "pow not implemented for {}",
                dt
            ))),
        }
    }
}
