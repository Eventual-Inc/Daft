use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn is_null(&self) -> DaftResult<Series> {
        self.inner.is_null()
        // use crate::array::ops::DaftIsNull;

        // with_match_daft_types!(self.data_type(), |$T| {
        //     Ok(DaftIsNull::is_null(&self.downcast::<$T>()?)?.into_series())
        // })
    }
}
