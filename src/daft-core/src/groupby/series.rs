use daft_common::error::DaftResult;
use crate::{series::Series, with_match_hashable_daft_types};

use super::{GroupIndicesPair, Indices, IntoGroups, IntoUniqueIdxs};

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
    fn make_unique_idxs(&self) -> DaftResult<Indices> {
        let s = self.as_physical()?;
        with_match_hashable_daft_types!(s.data_type(), |$T| {
            let array = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            array.make_unique_idxs()
        })
    }
}
