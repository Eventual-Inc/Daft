use crate::{error::DaftResult, series::Series, with_match_numeric_daft_types};

use crate::{array::DataArray, datatypes::{DaftIntegerType, DaftNumericType, NumericNative, Int64Type}};
use crate::array::BaseArray;

impl Series {
    pub fn sum(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftNumericAgg;
        // with_match_numeric_daft_types!(self.data_type(), |$T| {
        let array = self.downcast::<Int64Type>()?;
        Ok(array.sum()?.into_series())
        // })
    }
}
