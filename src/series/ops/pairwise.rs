use crate::{
    datatypes::UInt64Array,
    error::{DaftError, DaftResult},
    series::Series,
    with_match_comparable_daft_types,
};

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

    pub fn pairwise_multi_equal(lhs: &[Series], rhs: &[Series]) -> DaftResult<(Series, Series)> {
        if lhs.len() != rhs.len() {
            return Err(DaftError::ValueError(format!(
                "number of series on left and right do not match, {} vs {}",
                lhs.len(),
                rhs.len()
            )));
        }
        use crate::array::ops::build_multi_array_bicompare;
        let comp = build_multi_array_bicompare(lhs, rhs, vec![false; lhs.len()].as_slice())?;

        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for i in 0..lhs[0].len() {
            for j in 0..rhs[0].len() {
                if comp(i, j).is_eq() {
                    left_idx.push(i as u64);
                    right_idx.push(j as u64);
                }
            }
        }
        let left_series = UInt64Array::from(("left_indices", left_idx));
        let right_series = UInt64Array::from(("right_indices", right_idx));
        Ok((left_series.into_series(), right_series.into_series()))
    }
}
