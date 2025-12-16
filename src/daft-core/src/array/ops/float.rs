use common_error::DaftResult;
use num_traits::Float;

use super::{DaftIsInf, DaftIsNan, DaftNotNan, as_arrow::AsArrow};
use crate::{
    array::DataArray,
    datatypes::{BooleanArray, BooleanType, DaftFloatType, DaftNumericType, NullType},
};

impl<T> DaftIsNan for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        let arrow_array = self.as_arrow2();
        let result_arrow_array = daft_arrow::array::BooleanArray::from_trusted_len_values_iter(
            arrow_array.values_iter().map(|v| v.is_nan()),
        )
        .with_validity(arrow_array.validity().cloned());
        Ok(BooleanArray::from((self.name(), result_arrow_array)))
    }
}

impl DaftIsNan for DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_nan(&self) -> Self::Output {
        // Entire array is null; since we don't consider nulls to be NaNs, return an all null (invalid) boolean array.
        Ok(BooleanArray::from((
            self.name(),
            daft_arrow::array::BooleanArray::from_slice(vec![false; self.len()]).with_validity(
                daft_arrow::buffer::wrap_null_buffer(Some(
                    daft_arrow::buffer::NullBuffer::new_null(self.len()),
                )),
            ),
        )))
    }
}

impl<T> DaftIsInf for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_inf(&self) -> Self::Output {
        let arrow_array = self.as_arrow2();
        let result_arrow_array = daft_arrow::array::BooleanArray::from_trusted_len_values_iter(
            arrow_array.values_iter().map(|v| v.is_infinite()),
        )
        .with_validity(arrow_array.validity().cloned());
        Ok(BooleanArray::from((self.name(), result_arrow_array)))
    }
}

impl DaftIsInf for DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_inf(&self) -> Self::Output {
        Ok(BooleanArray::from((
            self.name(),
            daft_arrow::array::BooleanArray::from_slice(vec![false; self.len()]).with_validity(
                daft_arrow::buffer::wrap_null_buffer(Some(
                    daft_arrow::buffer::NullBuffer::new_null(self.len()),
                )),
            ),
        )))
    }
}

impl<T> DaftNotNan for DataArray<T>
where
    T: DaftFloatType,
    <T as DaftNumericType>::Native: Float,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_nan(&self) -> Self::Output {
        let arrow_array = self.as_arrow2();
        let result_arrow_array = daft_arrow::array::BooleanArray::from_trusted_len_values_iter(
            arrow_array.values_iter().map(|v| !v.is_nan()),
        )
        .with_validity(arrow_array.validity().cloned());
        Ok(BooleanArray::from((self.name(), result_arrow_array)))
    }
}

impl DaftNotNan for DataArray<NullType> {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_nan(&self) -> Self::Output {
        // Entire array is null; since we don't consider nulls to be NaNs, return an all null (invalid) boolean array.
        Ok(BooleanArray::from((
            self.name(),
            daft_arrow::array::BooleanArray::from_slice(vec![false; self.len()]).with_validity(
                daft_arrow::buffer::wrap_null_buffer(Some(
                    daft_arrow::buffer::NullBuffer::new_null(self.len()),
                )),
            ),
        )))
    }
}
