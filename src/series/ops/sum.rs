use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
    pub fn sum(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftNumericAgg;
        // with_match_numeric_daft_types!(self.data_type(), |$T| {

        use crate::datatypes::DataType::*;

        match self.data_type() {
            // intX -> int64 (in line with numpy)
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.cast(&Int64)?;
                Ok(casted.i64()?.sum()?.into_series())
            }
            // uintX -> uint64 (in line with numpy)
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.cast(&UInt64)?;
                Ok(casted.u64()?.sum()?.into_series())
            }
            // floatX -> floatX (in line with numpy)
            Float32 => Ok(self.downcast::<Float32Type>()?.sum()?.into_series()),
            Float64 => Ok(self.downcast::<Float64Type>()?.sum()?.into_series()),
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
        // let array = self.downcast::<$T>()?;
        // Ok(array.sum()?.into_series())
        // })
    }
}
