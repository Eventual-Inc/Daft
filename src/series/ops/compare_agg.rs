use crate::{error::DaftResult, series::Series, with_match_comparable_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn min(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        with_match_comparable_daft_types!(self.data_type(), |$T| {
            Ok(DaftCompareAggable::min(&self.downcast::<$T>()?)?.into_series())
        })
    }

    pub fn max(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        with_match_comparable_daft_types!(self.data_type(), |$T| {
            Ok(DaftCompareAggable::max(&self.downcast::<$T>()?)?.into_series())
        })
    }
}
