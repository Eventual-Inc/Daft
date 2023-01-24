use std::sync::Arc;

use crate::datatypes::{
    BooleanArray, DaftDataType, DaftNumericType, DataType, Field, Utf8Array, Utf8Type,
};

use crate::array::DataArray;
use crate::error::{DaftError, DaftResult};

impl<T: DaftNumericType> From<Box<arrow2::array::PrimitiveArray<T::Native>>> for DataArray<T> {
    fn from(item: Box<arrow2::array::PrimitiveArray<T::Native>>) -> Self {
        DataArray::new(
            Field::new("arrow_array", T::get_dtype()).into(),
            item.arced(),
        )
        .unwrap()
    }
}

impl From<Box<arrow2::array::Utf8Array<i64>>> for Utf8Array {
    fn from(item: Box<arrow2::array::Utf8Array<i64>>) -> Self {
        DataArray::new(
            Field::new("arrow_array", DataType::Utf8).into(),
            item.arced(),
        )
        .unwrap()
    }
}

impl<T> From<&[T::Native]> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(slice: &[T::Native]) -> Self {
        let arrow_array = arrow2::array::PrimitiveArray::<T::Native>::from_slice(slice);
        DataArray::new(
            Field::new("arrow_array", T::get_dtype()).into(),
            arrow_array.arced(),
        )
        .unwrap()
    }
}

impl From<&[bool]> for BooleanArray {
    fn from(slice: &[bool]) -> Self {
        let arrow_array = arrow2::array::BooleanArray::from_slice(slice);
        DataArray::new(
            Field::new("arrow_array", DataType::Boolean).into(),
            arrow_array.arced(),
        )
        .unwrap()
    }
}

impl<T: AsRef<str>> From<&[T]> for DataArray<Utf8Type> {
    fn from(slice: &[T]) -> Self {
        let arrow_array = arrow2::array::Utf8Array::<i64>::from_slice(slice);
        DataArray::new(
            Field::new("arrow_array", DataType::Utf8).into(),
            arrow_array.arced(),
        )
        .unwrap()
    }
}

impl<T: DaftDataType> TryFrom<Box<dyn arrow2::array::Array>> for DataArray<T> {
    type Error = DaftError;

    fn try_from(item: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        let self_arrow_type = T::get_dtype().to_arrow().unwrap();
        if !item.data_type().eq(&self_arrow_type) {
            return Err(DaftError::TypeError(format!(
                "mismatch in expected data type {:?} vs {:?}",
                item.data_type(),
                self_arrow_type
            )));
        }
        DataArray::new(
            Field::new("arrow_array", T::get_dtype()).into(),
            Arc::from(item),
        )
    }
}
