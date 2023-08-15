use crate::{datatypes::UInt64Array, series::Series, with_match_comparable_daft_types};
use common_error::DaftResult;

impl Series {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let s = self.as_physical()?;
        with_match_comparable_daft_types!(s.data_type(), |$T| {
            let downcasted = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            downcasted.hash(seed)
        })
    }
}
