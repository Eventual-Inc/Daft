use crate::error::DaftError;
use crate::{error::DaftResult, series::Series, with_match_comparable_daft_types};

use crate::array::BaseArray;

impl Series {
    pub fn argsort(&self, descending: bool) -> DaftResult<Series> {
        let series = self.as_physical()?;

        with_match_comparable_daft_types!(series.data_type(), |$T| {
            let downcasted = series.downcast::<$T>()?;
            Ok(downcasted.argsort::<UInt64Type>(descending)?.into_series())
        })
    }

    pub fn argsort_multikey(sort_keys: &[Series], descending: &[bool]) -> DaftResult<Series> {
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
                .argsort(*descending.first().unwrap());
        }

        let first = sort_keys.first().unwrap().as_physical()?;
        with_match_comparable_daft_types!(first.data_type(), |$T| {
            let downcasted = first.downcast::<$T>()?;
            let result = downcasted.argsort_multikey::<UInt64Type>(&sort_keys[1..], descending)?;
            Ok(result.into_series())
        })
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            let downcasted = s.downcast::<$T>()?;
            downcasted.sort(descending)?.into_series()
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }

        Ok(result)
    }
}
