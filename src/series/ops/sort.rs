use crate::{error::DaftResult, series::Series, with_match_comparable_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn argsort(&self, descending: bool) -> DaftResult<Series> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            let downcasted = self.downcast::<$T>()?;
            Ok(downcasted.argsort::<UInt64Type>(descending)?.into_series())
        })
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            let downcasted = self.downcast::<$T>()?;
            Ok(downcasted.sort(descending)?.into_series())
        })
    }
}
