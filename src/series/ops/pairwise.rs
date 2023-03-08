use crate::{error::DaftResult, series::Series, with_match_comparable_daft_types};

use super::match_types_on_series;

use crate::array::BaseArray;

impl Series {
    pub fn pairwise_equal(&self, rhs: &Self) -> DaftResult<(Series, Series)> {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            let (lidx, ridx) = lhs.pairwise_compare(rhs, |l,r| l == r)?;
            Ok((lidx.into_series(), ridx.into_series()))
        })
    }
}
