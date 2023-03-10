use crate::{error::DaftResult, series::Series, with_match_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn count(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCountAggable;

        with_match_daft_types!(self.data_type(), |$T| {
            Ok(DaftCountAggable::count(&self.downcast::<$T>()?)?.into_series())
        })
    }
}
