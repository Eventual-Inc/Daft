use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;
impl Series {
    pub fn log2(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
                let s = self.cast(&DataType::Float64)?;
                Ok(s.f64()?.log2()?.into_series())
            }
            Float32 => Ok(self.f32()?.log2()?.into_series()),
            Float64 => Ok(self.f64()?.log2()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log2 not implemented for {}",
                dt
            ))),
        }
    }

    pub fn log10(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
                let s = self.cast(&DataType::Float64)?;
                Ok(s.f64()?.log10()?.into_series())
            }
            Float32 => Ok(self.f32()?.log10()?.into_series()),
            Float64 => Ok(self.f64()?.log10()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log10 not implemented for {}",
                dt
            ))),
        }
    }

    pub fn log(&self, base: f64) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
                let s = self.cast(&DataType::Float64)?;
                Ok(s.f64()?.log(base)?.into_series())
            }
            Float32 => Ok(self.f32()?.log(base as f32)?.into_series()),
            Float64 => Ok(self.f64()?.log(base)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "log not implemented for {}",
                dt
            ))),
        }
    }

    pub fn ln(&self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        use DataType::*;
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => {
                let s = self.cast(&DataType::Float64)?;
                Ok(s.f64()?.ln()?.into_series())
            }
            Float32 => Ok(self.f32()?.ln()?.into_series()),
            Float64 => Ok(self.f64()?.ln()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "ln not implemented for {}",
                dt
            ))),
        }
    }
}
