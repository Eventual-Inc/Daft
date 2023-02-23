use crate::{
    array::ops::{DaftCompare, DaftLogical},
    datatypes::{BooleanArray, BooleanType, DataType},
    error::{DaftError, DaftResult},
    series::Series,
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
            lhs.equal(rhs)
        })
    }

    fn not_equal(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            lhs.not_equal(rhs)
        })
    }

    fn lt(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            lhs.lt(rhs)
        })
    }

    fn lte(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            lhs.lte(rhs)
        })
    }

    fn gt(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            lhs.gt(rhs)
        })
    }

    fn gte(&self, rhs: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;
        with_match_comparable_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            lhs.gte(rhs)
        })
    }
}

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<BooleanArray>;
    fn and(&self, rhs: &Series) -> Self::Output {
        if self.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                self.data_type(),
                self.name()
            )));
        } else if rhs.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                rhs.data_type(),
                rhs.name()
            )));
        }
        return self
            .downcast::<BooleanType>()?
            .and(rhs.downcast::<BooleanType>()?);
    }
    fn or(&self, rhs: &Series) -> Self::Output {
        if self.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                self.data_type(),
                self.name()
            )));
        } else if rhs.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                rhs.data_type(),
                rhs.name()
            )));
        }
        return self
            .downcast::<BooleanType>()?
            .or(rhs.downcast::<BooleanType>()?);
    }

    fn xor(&self, rhs: &Series) -> Self::Output {
        if self.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                self.data_type(),
                self.name()
            )));
        } else if rhs.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                rhs.data_type(),
                rhs.name()
            )));
        }
        return self
            .downcast::<BooleanType>()?
            .xor(rhs.downcast::<BooleanType>()?);
    }
}
