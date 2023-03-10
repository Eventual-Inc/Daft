use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
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
    pub fn mean(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftNumericAggable;
        use crate::datatypes::DataType::*;

        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.cast(&Int64)?;
                Ok(DaftNumericAggable::mean(&casted.i64()?)?.into_series())
            }
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.cast(&UInt64)?;
                Ok(DaftNumericAggable::mean(&casted.u64()?)?.into_series())
            }
            Float32 => {
                Ok(DaftNumericAggable::mean(&self.downcast::<Float32Type>()?)?.into_series())
            }
            Float64 => {
                Ok(DaftNumericAggable::mean(&self.downcast::<Float64Type>()?)?.into_series())
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }
}
