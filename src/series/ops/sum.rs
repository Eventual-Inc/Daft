use crate::{error::DaftResult, series::Series, with_match_numeric_and_utf_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn sum(&self) -> DaftResult<Series> {
        with_match_numeric_and_utf_daft_types!(self.data_type(), |$T| {
            let array = self.downcast::<$T>()?;
            Ok(array.sum()?.into_series())
        })
    }
}
