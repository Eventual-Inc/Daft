use crate::{
    datatypes::UInt64Array,
    series::{ops::match_types_on_series, Series},
    with_match_comparable_daft_types,
};
use common_error::DaftResult;

impl Series {
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let (lhs, rhs) = match_types_on_series(self, keys)?;
        let lhs = lhs.as_physical()?;
        let rhs = rhs.as_physical()?;

        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            let rhs = rhs.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            lhs.search_sorted(rhs, descending)
        })
    }
}
