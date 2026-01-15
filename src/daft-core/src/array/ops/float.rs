use arrow::datatypes::ArrowPrimitiveType;
use common_error::DaftResult;
use num_traits::Float;

use super::{DaftIsInf, DaftIsNan, DaftNotNan, full::FullNull};
use crate::{
    array::DataArray,
    datatypes::{
        BooleanArray, BooleanType, DaftFloatType, DaftNumericType, DataType, NullType,
        NumericNative,
    },
};

impl<T> DaftIsNan for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        let result =
            BooleanArray::from_values(self.name(), self.values().iter().map(|v| v.is_nan()));
        result.with_nulls(self.nulls().cloned())
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
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_inf(&self) -> Self::Output {
        let result =
            BooleanArray::from_values(self.name(), self.values().iter().map(|v| v.is_infinite()));
        result.with_nulls(self.nulls().cloned())
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
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_nan(&self) -> Self::Output {
        let result =
            BooleanArray::from_values(self.name(), self.values().iter().map(|v| !v.is_nan()));
        result.with_nulls(self.nulls().cloned())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::Float64Array;

    #[test]
    fn test_is_nan() {
        let arr = Float64Array::from(("a", vec![1.0, f64::NAN, 3.0, f64::NAN]));
        let result = arr.is_nan().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.get(0), Some(false));
        assert_eq!(result.get(1), Some(true));
        assert_eq!(result.get(2), Some(false));
        assert_eq!(result.get(3), Some(true));
    }

    #[test]
    fn test_is_inf() {
        let arr = Float64Array::from(("a", vec![1.0, f64::INFINITY, f64::NEG_INFINITY, 0.0]));
        let result = arr.is_inf().unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(result.get(0), Some(false));
        assert_eq!(result.get(1), Some(true));
        assert_eq!(result.get(2), Some(true));
        assert_eq!(result.get(3), Some(false));
    }

    #[test]
    fn test_not_nan() {
        let arr = Float64Array::from(("a", vec![1.0, f64::NAN, 3.0]));
        let result = arr.not_nan().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some(true));
        assert_eq!(result.get(1), Some(false));
        assert_eq!(result.get(2), Some(true));
    }
}
