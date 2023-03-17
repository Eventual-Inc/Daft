use crate::{
    datatypes::UInt64Array, error::DaftResult, series::Series, with_match_comparable_daft_types,
};

impl Series {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let s = self.as_physical()?;
        with_match_comparable_daft_types!(s.data_type(), |$T| {
            let downcasted = s.downcast::<$T>()?;
            downcasted.hash(seed)
        })
    }
}
