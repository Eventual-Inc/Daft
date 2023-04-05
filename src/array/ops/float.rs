use std::sync::Arc;

use crate::{
    array::DataArray,
    datatypes::{BooleanType, DaftFloatType, DaftNumericType, DataType, Field, NullType},
    error::DaftResult,
};
use num_traits::Float;

use super::DaftIsNan;

impl<T> DaftIsNan for &DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        let arrow_array = self.downcast();
        let result_arrow_array = arrow2::array::BooleanArray::from_trusted_len_values_iter(
            arrow_array.values_iter().map(|v| v.is_nan()),
        )
        .with_validity(arrow_array.validity().cloned());
        DataArray::<BooleanType>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::Boolean)),
            Arc::new(result_arrow_array),
        )
    }
}

impl DaftIsNan for &DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        // Entire array is null; since we don't consider nulls to be NaNs, return an all null (invalid) boolean array.
        DataArray::<BooleanType>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::Boolean)),
            Arc::new(
                arrow2::array::BooleanArray::from_slice(vec![false; self.len()])
                    .with_validity(Some(arrow2::bitmap::Bitmap::from(vec![false; self.len()]))),
            ),
        )
    }
}
