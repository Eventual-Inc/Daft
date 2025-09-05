use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{Series, array_impl::IntoSeries},
};

impl Series {
    pub fn log2(&self) -> DaftResult<Self> {
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
                Ok(s.f64()?.log2()?.into_series())
            }
            DataType::Float32 => Ok(self.f32()?.log2()?.into_series()),
            DataType::Float64 => Ok(self.f64()?.log2()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log2 not implemented for {}",
                dt
            ))),
        }
    }

    pub fn log10(&self) -> DaftResult<Self> {
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
                Ok(s.f64()?.log10()?.into_series())
            }
            DataType::Float32 => Ok(self.f32()?.log10()?.into_series()),
            DataType::Float64 => Ok(self.f64()?.log10()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log10 not implemented for {}",
                dt
            ))),
        }
    }

    pub fn log(&self, base: f64) -> DaftResult<Self> {
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
                Ok(s.f64()?.log(base)?.into_series())
            }
            DataType::Float32 => Ok(self.f32()?.log(base as f32)?.into_series()),
            DataType::Float64 => Ok(self.f64()?.log(base)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log not implemented for {}",
                dt
            ))),
        }
    }

    pub fn ln(&self) -> DaftResult<Self> {
        use crate::series::array_impl::IntoSeries;
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
                Ok(s.f64()?.ln()?.into_series())
            }
            DataType::Float32 => Ok(self.f32()?.ln()?.into_series()),
            DataType::Float64 => Ok(self.f64()?.ln()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "ln not implemented for {}",
                dt
            ))),
        }
    }

    pub fn log1p(&self) -> DaftResult<Self> {
        use crate::series::array_impl::IntoSeries;
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
                Ok(s.f64()?.log1p()?.into_series())
            }
            DataType::Float32 => Ok(self.f32()?.log1p()?.into_series()),
            DataType::Float64 => Ok(self.f64()?.log1p()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log1p not implemented for {}",
                dt
            ))),
        }
    }
}
