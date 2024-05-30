use super::{full::FullNull, DaftBetween, DaftCompare, DaftLogical};
use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftIntegerType, DaftNumericType, NullArray},
    DataType,
};
use common_error::{DaftError, DaftResult};

use num_traits::ToPrimitive;

impl<T, Scalar> DaftBetween<Scalar, Scalar> for DataArray<T>
where
    T: DaftNumericType,
    Scalar: ToPrimitive,
{
    type Output = DaftResult<BooleanArray>;

    fn between(&self, lower: Scalar, upper: Scalar) -> Self::Output {
        let gte_res = self.gte(lower);
        let lte_res = self.lte(upper);
        gte_res.and(&lte_res)
    }
}

impl<T> DaftBetween<&DataArray<T>, &DataArray<T>> for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<BooleanArray>;

    fn between(&self, lower: &DataArray<T>, upper: &DataArray<T>) -> Self::Output {
        match  (self.len(), lower.len(), upper.len()) {
            (v_size, l_size, u_size) if (v_size == l_size && v_size == u_size) || (l_size == 1 && u_size == 1) => {
                let gte_res = self.gte(lower)?;
                let lte_res = self.lte(upper)?;
                gte_res.and(&lte_res)
            },
            (v_size, l_size, u_size) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {v_size} vs {}: {l_size} vs {}: {u_size}",
                self.name(),
                lower.name(),
                upper.name()
            ))),
        }
    }
}

impl DaftBetween<&NullArray, &NullArray> for NullArray {
    type Output = DaftResult<BooleanArray>;

    fn between(&self, _lower: &NullArray, _upper: &NullArray) -> Self::Output {
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}

impl<T> DaftBetween<&DataArray<T>, &NullArray> for NullArray
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord,
    <T as DaftNumericType>::Native: std::hash::Hash,
    <T as DaftNumericType>::Native: std::cmp::Eq,
{
    type Output = DaftResult<BooleanArray>;

    fn between(&self, _lower: &DataArray<T>, _upper: &NullArray) -> Self::Output {
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}

impl<T> DaftBetween<&NullArray, &DataArray<T>> for NullArray
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord,
    <T as DaftNumericType>::Native: std::hash::Hash,
    <T as DaftNumericType>::Native: std::cmp::Eq,
{
    type Output = DaftResult<BooleanArray>;

    fn between(&self, _lower: &NullArray, _upper: &DataArray<T>) -> Self::Output {
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::ops::{full::FullNull, DaftBetween},
        datatypes::Int64Array,
        DataType,
    };
    use common_error::DaftResult;

    #[test]
    fn test_between_two_scalars() -> DaftResult<()> {
        let value = Int64Array::arange("a", 1, 5, 1)?;
        assert_eq!(value.len(), 4);
        let result: Vec<_> = value.between(1, 2)?.into_iter().collect();
        assert_eq!(
            result[..],
            [Some(true), Some(true), Some(false), Some(false)]
        );
        Ok(())
    }

    #[test]
    fn test_between_two_arrays_of_same_size() -> DaftResult<()> {
        let value = Int64Array::arange("value", 1, 4, 1)?;
        let lower = Int64Array::arange("lower", 0, 6, 2)?;
        let upper = Int64Array::arange("upper", -2, 8, 4)?;
        let result: Vec<_> = value.between(&lower, &upper)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn test_between_all_null_arrays() -> DaftResult<()> {
        let value = Int64Array::full_null("value", &DataType::Int64, 2);
        let lower = Int64Array::full_null("lower", &DataType::Int64, 2);
        let upper = Int64Array::full_null("upper", &DataType::Int64, 2);
        let result: Vec<_> = value.between(&lower, &upper)?.into_iter().collect();
        assert_eq!(result[..], [None, None]);
        Ok(())
    }
}
