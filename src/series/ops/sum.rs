use std::sync::Arc;

use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
    pub fn count(&self) -> DaftResult<Series> {
        // let data_type = self.data_type();
        let data_array = self.array();
        let arrow_array = data_array.data();
        let count = match arrow_array.validity() {
            None => arrow_array.len(),
            Some(bitmap) => arrow_array.len() - bitmap.unset_bits(),
        };
        let arrow_array = arrow2::array::PrimitiveArray::from([Some(count as u64)]);
        Ok(DataArray::<UInt64Type>::new(
            Arc::new(data_array.field().clone()),
            Arc::new(arrow_array),
        )?
        .into_series())
    }

    pub fn sum(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftNumericAgg;
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
    }
}
