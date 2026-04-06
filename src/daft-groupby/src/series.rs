use common_error::DaftResult;
use daft_core::{
    array::ops::{GroupIndicesPair, VecIndices},
    series::Series,
    with_match_hashable_daft_types,
};

use crate::{IntoGroups, IntoUniqueIdxs};

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
