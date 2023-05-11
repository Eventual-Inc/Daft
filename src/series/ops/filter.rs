use crate::{
    datatypes::BooleanArray,
    error::{DaftError, DaftResult},
    series::Series,
    with_match_physical_daft_types,
};

impl Series {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Series> {
        match (self.len(), mask.len()) {
            (_, 1) => {
                // if Some(true) == mask.get(0) {
                //     Ok(self.clone())
                // } else {
                //     Series::empty(self.name(), self.data_type())
                // }
                self.inner.filter(mask)
            }
            (n, m) if n == m => {
                self.inner.filter(mask)
                // let s = self.as_physical()?;

                // let result = with_match_physical_daft_types!(s.data_type(), |$T| {
                //     let downcasted = s.downcast::<$T>()?;
                //     downcasted.filter(mask)?.into_series()
                // });
                // if result.data_type() != self.data_type() {
                //     return result.cast(self.data_type());
                // }
                // Ok(result)
            }
            _ => Err(DaftError::ValueError(format!(
                "Lengths for filter do not match, Series {} vs mask {}",
                self.len(),
                mask.len()
            ))),
        }
    }
}
