use crate::{
    array::ops::{GroupIndicesPair, IntoGroups},
    series::Series,
    with_match_hashable_daft_types,
};
use common_error::DaftResult;

impl IntoGroups for Series {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair> {
        let s = self.as_physical()?;
        with_match_hashable_daft_types!(s.data_type(), |$T| {
            let array = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            array.make_groups()
        })
    }
}
