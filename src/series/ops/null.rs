use crate::{error::DaftResult, series::Series, with_match_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn is_null(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftIsNull;

        with_match_daft_types!(self.data_type(), |$T| {
            Ok(DaftIsNull::is_null(&self.downcast::<$T>()?)?.into_series())
        })
    }
}
