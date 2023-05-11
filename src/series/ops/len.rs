use crate::error::DaftResult;
use crate::{series::Series, with_match_physical_daft_types};

impl Series {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    // pub fn size_bytes(&self) -> DaftResult<usize> {
    //     let s = self.as_physical()?;

    //     with_match_physical_daft_types!(s.data_type(), |$T| {
    //         let downcasted = s.downcast::<$T>()?;
    //         Ok(downcasted.size_bytes())
    //     })
    // }
}
