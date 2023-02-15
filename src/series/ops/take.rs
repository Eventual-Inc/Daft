use crate::array::{BaseArray, DataArray};
use crate::datatypes::{DaftIntegerType, DaftNumericType};
use crate::{
    error::DaftResult, series::Series, with_match_comparable_daft_types,
    with_match_integer_daft_types,
};

impl Series {
    pub fn head(&self, num: usize) -> DaftResult<Series> {
        if num >= self.len() {
            return Ok(self.clone());
        }

        with_match_comparable_daft_types!(self.data_type(), |$T| {
            Ok(self.downcast::<$T>()?.head(num)?.into_series())
        })
    }

    pub fn take(&self, idx: &Series) -> DaftResult<Series> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            with_match_integer_daft_types!(idx.data_type(), |$S| {
                Ok(self.downcast::<$T>()?.take(idx.downcast::<$S>()?)?.into_series())
            })
        })
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            self.downcast::<$T>()?.str_value(idx)
        })
    }
}
