use crate::{error::DaftResult, series::Series, with_match_numeric_and_utf_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn broadcast(&self, num: usize) -> DaftResult<Series> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Series named: {}",
                self.name()
            )));
        }

        with_match_numeric_and_utf_daft_types!(self.data_type(), |$T| {
            let array = self.downcast::<$T>()?;
            Ok(array.broadcast(num)?.into_series())
        })
    }
}
