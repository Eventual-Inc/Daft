use crate::datatypes::DataType;
use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn shift_left(&self, bits: &Self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        if !bits.data_type().is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected input to shift_left to be integer, got {}",
                bits.data_type()
            )));
        }
        let is_negative = match bits.data_type() {
            DataType::Int8 => bits
                .i8()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            DataType::Int16 => bits
                .i16()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            DataType::Int32 => bits
                .i32()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            DataType::Int64 => bits
                .i64()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            _ => false,
        };
        if is_negative {
            return Err(DaftError::ValueError(
                "Cannot shift left by a negative number".to_string(),
            ));
        }
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
                "Expected input to shift_left to be integer, got {}",
                dt
            ))),
        }
    }

    pub fn shift_right(&self, bits: &Self) -> DaftResult<Series> {
        use crate::series::array_impl::IntoSeries;
        if !bits.data_type().is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected input to shift_right to be integer, got {}",
                bits.data_type()
            )));
        }
        let is_negative = match bits.data_type() {
            DataType::Int8 => bits
                .i8()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            DataType::Int16 => bits
                .i16()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            DataType::Int32 => bits
                .i32()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            DataType::Int64 => bits
                .i64()?
                .into_iter()
                .any(|x| x.is_some_and(|x| x.is_negative())),
            _ => false,
        };
        if is_negative {
            return Err(DaftError::ValueError(
                "Cannot shift right by a negative number".to_string(),
            ));
        }
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
                "Expected input to shift_right to be integer, got {}",
                dt
            ))),
        }
    }
}
