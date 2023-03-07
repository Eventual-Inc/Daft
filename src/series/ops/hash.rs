use crate::{
    datatypes::UInt64Array, error::DaftResult, series::Series, with_match_comparable_daft_types,
};

impl Series {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            let downcasted = self.downcast::<$T>()?;
            downcasted.hash(seed)
        })
    }
}
