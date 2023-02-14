use crate::array::BaseArray;
use crate::{error::DaftResult, series::Series, with_match_comparable_daft_types};

impl Series {
    pub fn head(&self, num: usize) -> DaftResult<Series> {
        if num >= self.len() {
            return Ok(self.clone());
        }

        with_match_comparable_daft_types!(self.data_type(), |$T| {
            Ok(self.downcast::<$T>()?.head(num)?.into_series())
        })
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            self.downcast::<$T>()?.str_value(idx)
        })
    }
}
