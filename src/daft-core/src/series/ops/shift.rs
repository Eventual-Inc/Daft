use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn shift_left(&self, bits: &Self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        let bits = bits.cast(&DataType::UInt64)?;
        match self.data_type() {
            DataType::Int8 => Ok(self.i8()?.shift_left(bits.u64()?)?.into_series()),
            DataType::Int16 => Ok(self.i16()?.shift_left(bits.u64()?)?.into_series()),
            DataType::Int32 => Ok(self.i32()?.shift_left(bits.u64()?)?.into_series()),
            DataType::Int64 => Ok(self.i64()?.shift_left(bits.u64()?)?.into_series()),
            DataType::UInt8 => Ok(self.u8()?.shift_left(bits.u64()?)?.into_series()),
            DataType::UInt16 => Ok(self.u16()?.shift_left(bits.u64()?)?.into_series()),
            DataType::UInt32 => Ok(self.u32()?.shift_left(bits.u64()?)?.into_series()),
            DataType::UInt64 => Ok(self.u64()?.shift_left(bits.u64()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Expected input to shift_left to be numeric, got {}",
                dt
            ))),
        }
    }

    pub fn shift_right(&self, bits: &Self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        let bits = bits.cast(&DataType::UInt64)?;
        match self.data_type() {
            DataType::Int8 => Ok(self.i8()?.shift_right(bits.u64()?)?.into_series()),
            DataType::Int16 => Ok(self.i16()?.shift_right(bits.u64()?)?.into_series()),
            DataType::Int32 => Ok(self.i32()?.shift_right(bits.u64()?)?.into_series()),
            DataType::Int64 => Ok(self.i64()?.shift_right(bits.u64()?)?.into_series()),
            DataType::UInt8 => Ok(self.u8()?.shift_right(bits.u64()?)?.into_series()),
            DataType::UInt16 => Ok(self.u16()?.shift_right(bits.u64()?)?.into_series()),
            DataType::UInt32 => Ok(self.u32()?.shift_right(bits.u64()?)?.into_series()),
            DataType::UInt64 => Ok(self.u64()?.shift_right(bits.u64()?)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "Expected input to shift_right to be numeric, got {}",
                dt
            ))),
        }
    }
}
