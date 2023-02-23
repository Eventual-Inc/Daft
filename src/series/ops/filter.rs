use crate::{
    datatypes::BooleanArray,
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;

impl Series {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Series> {
        if self.len() != mask.len() {
            return Err(DaftError::ValueError(format!(
                "Lengths for filter do not match, Series {} vs mask {}",
                self.len(),
                mask.len()
            )));
        }
        use crate::with_match_comparable_daft_types;

        with_match_comparable_daft_types!(self.data_type(), |$T| {
            let downcasted = self.downcast::<$T>()?;
            Ok(downcasted.filter(mask)?.into_series())
        })
    }
}
