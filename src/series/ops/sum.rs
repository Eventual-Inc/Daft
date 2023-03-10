use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
    pub fn count(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCountAggable;
        match self.data_type() {
            DataType::Null => {
                Ok(DaftCountAggable::count(&self.downcast::<NullType>()?)?.into_series())
            }
            DataType::Boolean => {
                Ok(DaftCountAggable::count(&self.downcast::<BooleanType>()?)?.into_series())
            }
            DataType::Int8 => {
                Ok(DaftCountAggable::count(&self.downcast::<Int8Type>()?)?.into_series())
            }
            DataType::Int16 => {
                Ok(DaftCountAggable::count(&self.downcast::<Int16Type>()?)?.into_series())
            }
            DataType::Int32 => {
                Ok(DaftCountAggable::count(&self.downcast::<Int32Type>()?)?.into_series())
            }
            DataType::Int64 => {
                Ok(DaftCountAggable::count(&self.downcast::<Int64Type>()?)?.into_series())
            }
            DataType::UInt8 => {
                Ok(DaftCountAggable::count(&self.downcast::<UInt8Type>()?)?.into_series())
            }
            DataType::UInt16 => {
                Ok(DaftCountAggable::count(&self.downcast::<UInt16Type>()?)?.into_series())
            }
            DataType::UInt32 => {
                Ok(DaftCountAggable::count(&self.downcast::<UInt32Type>()?)?.into_series())
            }
            DataType::UInt64 => {
                Ok(DaftCountAggable::count(&self.downcast::<UInt64Type>()?)?.into_series())
            }
            DataType::Float16 => {
                Ok(DaftCountAggable::count(&self.downcast::<Float16Type>()?)?.into_series())
            }
            DataType::Float32 => {
                Ok(DaftCountAggable::count(&self.downcast::<Float32Type>()?)?.into_series())
            }
            DataType::Float64 => {
                Ok(DaftCountAggable::count(&self.downcast::<Float64Type>()?)?.into_series())
            }
            DataType::Timestamp(_, _) => {
                Ok(DaftCountAggable::count(&self.downcast::<TimestampType>()?)?.into_series())
            }
            DataType::Date => {
                Ok(DaftCountAggable::count(&self.downcast::<DateType>()?)?.into_series())
            }
            DataType::Time(_) => {
                Ok(DaftCountAggable::count(&self.downcast::<TimeType>()?)?.into_series())
            }
            DataType::Duration(_) => {
                Ok(DaftCountAggable::count(&self.downcast::<DurationType>()?)?.into_series())
            }
            DataType::Binary => {
                Ok(DaftCountAggable::count(&self.downcast::<BinaryType>()?)?.into_series())
            }
            DataType::Utf8 => {
                Ok(DaftCountAggable::count(&self.downcast::<Utf8Type>()?)?.into_series())
            }
            DataType::FixedSizeList(_, _) => {
                Ok(DaftCountAggable::count(&self.downcast::<FixedSizeListType>()?)?.into_series())
            }
            DataType::List(_) => {
                Ok(DaftCountAggable::count(&self.downcast::<ListType>()?)?.into_series())
            }
            DataType::Struct(_) => {
                Ok(DaftCountAggable::count(&self.downcast::<StructType>()?)?.into_series())
            }
            _ => panic!(),
        }
    }

    pub fn sum(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftNumericAggable;
        use crate::datatypes::DataType::*;

        match self.data_type() {
            // intX -> int64 (in line with numpy)
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.cast(&Int64)?;
                Ok(DaftNumericAggable::sum(&casted.i64()?)?.into_series())
            }
            // uintX -> uint64 (in line with numpy)
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.cast(&UInt64)?;
                Ok(DaftNumericAggable::sum(&casted.u64()?)?.into_series())
            }
            // floatX -> floatX (in line with numpy)
            Float32 => Ok(DaftNumericAggable::sum(&self.downcast::<Float32Type>()?)?.into_series()),
            Float64 => Ok(DaftNumericAggable::sum(&self.downcast::<Float64Type>()?)?.into_series()),
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }
}
