use crate::{
    datatypes::UInt64Array,
    series::{ops::match_types_on_series, Series},
    with_match_comparable_daft_types,
};
use common_error::DaftResult;

impl Series {
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let series = match_types_on_series(vec![self, keys])?;
        let lhs = series[0].as_physical()?;
        let rhs = series[1].as_physical()?;

        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            let rhs = rhs.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            lhs.search_sorted(rhs, descending)
        })
    }
}
