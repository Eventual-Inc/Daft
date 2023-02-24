use crate::{
    datatypes::BooleanArray,
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::array::BaseArray;
use crate::with_match_comparable_daft_types;

impl Series {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Series> {
        match (self.len(), mask.len()) {
            (_, 1) => {
                if Some(true) == mask.get(0) {
                    Ok(self.clone())
                } else {
                    Ok(BooleanArray::empty(self.name()).into_series())
                }
            }
            (n, m) if n == m => {
                with_match_comparable_daft_types!(self.data_type(), |$T| {
                    let downcasted = self.downcast::<$T>()?;
                    Ok(downcasted.filter(mask)?.into_series())
                })
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Lengths for filter do not match, Series {} vs mask {}",
                    self.len(),
                    mask.len()
                )));
            }
        }
    }
}
