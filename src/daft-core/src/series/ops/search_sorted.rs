use std::sync::Arc;

use daft_common_error::DaftResult;

use crate::{
    datatypes::{DataType, Field, UInt64Array},
    kernels::search_sorted as kernels_search_sorted,
    series::{Series, ops::cast_series_to_supertype},
};

impl Series {
    pub fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array> {
        let casted_series = cast_series_to_supertype(&[self, keys])?;
        assert!(casted_series.len() == 2);

        let lhs = casted_series[0].as_physical()?;
        let rhs = casted_series[1].as_physical()?;
        let result = kernels_search_sorted::search_sorted(
            lhs.to_arrow()?.as_ref(),
            rhs.to_arrow()?.as_ref(),
            descending,
        )?;
        UInt64Array::from_arrow(Field::new(lhs.name(), DataType::UInt64), Arc::new(result))
    }
}
