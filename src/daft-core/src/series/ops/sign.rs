use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{array_impl::IntoSeries, Series},
};

impl Series {
    pub fn sign(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::UInt8 => Ok(self.u8().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt16 => Ok(self.u16().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt32 => Ok(self.u32().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt64 => Ok(self.u64().unwrap().sign_unsigned()?.into_series()),
            DataType::Int8 => Ok(self.i8().unwrap().sign()?.into_series()),
            DataType::Int16 => Ok(self.i16().unwrap().sign()?.into_series()),
            DataType::Int32 => Ok(self.i32().unwrap().sign()?.into_series()),
            DataType::Int64 => Ok(self.i64().unwrap().sign()?.into_series()),
            DataType::Float32 => Ok(self.f32().unwrap().sign()?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().sign()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "sign not implemented for {}",
                dt
            ))),
        }
    }

    pub fn signum(&self) -> DaftResult<Self> {
        // Reuse sign implementation since they're identical
        self.sign()
    }

    pub fn negate(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::UInt8 => Ok(self
                .cast(&DataType::Int8)?
                .i8()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt8)?),
            DataType::UInt16 => Ok(self
                .cast(&DataType::Int16)?
                .i16()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt16)?),
            DataType::UInt32 => Ok(self
                .cast(&DataType::Int32)?
                .i32()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt32)?),
            DataType::UInt64 => Ok(self
                .cast(&DataType::Int64)?
                .i64()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt64)?),
            DataType::Int8 => Ok(self.i8().unwrap().negative()?.into_series()),
            DataType::Int16 => Ok(self.i16().unwrap().negative()?.into_series()),
            DataType::Int32 => Ok(self.i32().unwrap().negative()?.into_series()),
            DataType::Int64 => Ok(self.i64().unwrap().negative()?.into_series()),
            DataType::Float32 => Ok(self.f32().unwrap().negative()?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().negative()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "negate not implemented for {}",
                dt
            ))),
        }
    }

    pub fn negative(&self) -> DaftResult<Self> {
        // Reuse negate implementation since they're identical
        self.negate()
    }
}
