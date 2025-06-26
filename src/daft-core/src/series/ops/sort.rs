use common_error::{DaftError, DaftResult};

use crate::{
    series::{array_impl::IntoSeries, Series},
    with_match_comparable_daft_types,
};

impl Series {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let series = self.as_physical()?;
        with_match_comparable_daft_types!(series.data_type(), |$T| {
            let downcasted = series.downcast::<<$T as DaftDataType>::ArrayType>()?;
            Ok(downcasted.argsort::<UInt64Type>(descending, nulls_first)?.into_series())
        })
    }

    pub fn argsort_multikey(
        sort_keys: &[Self],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<Self> {
        if sort_keys.len() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "sort_keys and descending length must match, got {} vs {}",
                sort_keys.len(),
                descending.len()
            )));
        }

        if sort_keys.len() == 1 {
            return sort_keys
                .first()
                .unwrap()
                .argsort(*descending.first().unwrap(), *nulls_first.first().unwrap());
        }

        let first = sort_keys.first().unwrap().as_physical()?;
        with_match_comparable_daft_types!(first.data_type(), |$T| {
            let downcasted = first.downcast::<<$T as DaftDataType>::ArrayType>()?;
            let result = downcasted.argsort_multikey::<UInt64Type>(&sort_keys[1..], descending, nulls_first)?;
            Ok(result.into_series())
        })
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        self.inner.sort(descending, nulls_first)
    }
}
