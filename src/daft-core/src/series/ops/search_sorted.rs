use crate::{
    datatypes::UInt64Array,
    series::{ops::cast_series_to_supertype, Series},
    with_match_comparable_daft_types,
};
use common_error::DaftResult;

impl Series {
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let casted_series = cast_series_to_supertype(&[self, keys])?;
        assert!(casted_series.len() == 2);

        let lhs = casted_series[0].as_physical()?;
        let rhs = casted_series[1].as_physical()?;

        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            let rhs = rhs.downcast::<<$T as DaftDataType>::ArrayType>().unwrap();
            lhs.search_sorted(rhs, descending)
        })
    }
}
