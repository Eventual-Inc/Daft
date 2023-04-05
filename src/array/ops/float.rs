use std::sync::Arc;

use crate::{
    array::DataArray,
    datatypes::{BooleanType, DataType, Field, Float32Type, Float64Type, NullType},
    error::DaftResult,
};

use super::DaftIsNan;

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

macro_rules! impl_daft_float_is_nan {
    ($T:ident) => {
        impl DaftIsNan for &DataArray<$T> {
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
    };
}

impl_daft_float_is_nan!(Float32Type);
impl_daft_float_is_nan!(Float64Type);
