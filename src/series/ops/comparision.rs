use crate::{
    array::ops::DaftCompare, datatypes::BooleanArray, error::DaftResult, series::Series,
    with_match_comparable_daft_types,
};

use super::match_types_on_series;

impl DaftCompare<&Series> for Series {
    type Output = DaftResult<BooleanArray>;
    fn equal(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.equal(rhs))
        })
    }

    fn not_equal(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.not_equal(rhs))
        })
    }

    fn lt(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.lt(rhs))
        })
    }

    fn lte(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.lte(rhs))
        })
    }

    fn gt(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.gt(rhs))
        })
    }

    fn gte(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.gte(rhs))
        })
    }
}
