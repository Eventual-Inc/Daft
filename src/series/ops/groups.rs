use crate::{
    array::ops::{GroupIndicesPair, IntoGroups},
    error::DaftResult,
    series::Series,
    with_match_comparable_daft_types,
};

impl IntoGroups for Series {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair> {
        let s = self.as_physical()?;
        with_match_comparable_daft_types!(s.data_type(), |$T| {
            let array = s.downcast::<$T>()?;
            array.make_groups()
        })
    }
}
