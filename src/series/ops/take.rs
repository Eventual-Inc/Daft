use crate::{error::DaftResult, series::Series, with_match_comparable_daft_types};

impl Series {
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            self.downcast::<$T>()?.str_value(idx)
        })
    }
}
