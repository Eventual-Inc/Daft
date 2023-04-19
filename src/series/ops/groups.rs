use crate::{
    array::ops::{GroupIndicesPair, IntoGroups},
    error::DaftResult,
    series::Series,
    with_match_comparable_daft_types,
};

impl IntoGroups for Series {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair> {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            let array = self.downcast::<$T>()?;
            array.make_groups()
        })
    }
}
