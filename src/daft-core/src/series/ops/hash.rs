use crate::{
    datatypes::{UInt32Array, UInt64Array},
    series::Series,
    with_match_comparable_daft_types, with_match_integer_daft_types,
};
use common_error::DaftResult;

impl Series {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let s = self.as_physical()?;
        with_match_comparable_daft_types!(s.data_type(), |$T| {
            let downcasted = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            downcasted.hash(seed)
        })
    }

    pub fn murmur3_32(&self) -> DaftResult<UInt32Array> {
        // TODO Should be all supported iceberg types

        with_match_integer_daft_types!(self.data_type(),|$T| {
            let downcasted = self.downcast::<<$T as DaftDataType>::ArrayType>()?;
            downcasted.murmur3_32()
        })
    }
}
