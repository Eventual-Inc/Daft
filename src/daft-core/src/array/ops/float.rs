use common_error::DaftResult;
use num_traits::Float;

use super::{DaftIsInf, DaftIsNan, DaftNotNan, full::FullNull};
use crate::{
    array::DataArray,
    datatypes::{BooleanArray, BooleanType, DaftFloatType, DaftNumericType, DataType, NullType},
};

impl<T> DaftIsNan for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        let result: Vec<Option<bool>> = self.into_iter().map(|v| v.map(|x| x.is_nan())).collect();
        Ok(BooleanArray::from((self.name(), result.as_slice())))
    }
}

impl DaftIsNan for DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        // Entire array is null; since we don't consider nulls to be NaNs, return an all null (invalid) boolean array.
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}

impl<T> DaftIsInf for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_inf(&self) -> Self::Output {
        let result: Vec<Option<bool>> = self
            .into_iter()
            .map(|v| v.map(|x| x.is_infinite()))
            .collect();
        Ok(BooleanArray::from((self.name(), result.as_slice())))
    }
}

impl DaftIsInf for DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_inf(&self) -> Self::Output {
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}

impl<T> DaftNotNan for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_nan(&self) -> Self::Output {
        let result: Vec<Option<bool>> = self.into_iter().map(|v| v.map(|x| !x.is_nan())).collect();
        Ok(BooleanArray::from((self.name(), result.as_slice())))
    }
}

impl DaftNotNan for DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_nan(&self) -> Self::Output {
        // Entire array is null; since we don't consider nulls to be NaNs, return an all null (invalid) boolean array.
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}
