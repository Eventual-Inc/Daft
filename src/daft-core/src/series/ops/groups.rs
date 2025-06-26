use common_error::DaftResult;

use crate::{
    array::ops::{GroupIndicesPair, IntoGroups, IntoUniqueIdxs, VecIndices},
    series::Series,
    with_match_hashable_daft_types,
};

impl IntoGroups for Series {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair> {
        let s = self.as_physical()?;
        with_match_hashable_daft_types!(s.data_type(), |$T| {
            let array = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            array.make_groups()
        })
    }
}

impl IntoUniqueIdxs for Series {
    fn make_unique_idxs(&self) -> DaftResult<VecIndices> {
        let s = self.as_physical()?;
        with_match_hashable_daft_types!(s.data_type(), |$T| {
            let array = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            array.make_unique_idxs()
        })
    }
}
