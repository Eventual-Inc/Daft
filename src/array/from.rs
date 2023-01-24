use std::sync::Arc;

use crate::datatypes::{
    BooleanArray, DaftDataType, DaftNumericType, DataType, Field, Utf8Array, Utf8Type,
};

use crate::array::DataArray;
use crate::error::{DaftError, DaftResult};

impl<T: DaftNumericType> From<(&str, Box<arrow2::array::PrimitiveArray<T::Native>>)>
    for DataArray<T>
{
    fn from(item: (&str, Box<arrow2::array::PrimitiveArray<T::Native>>)) -> Self {
        let (name, array) = item;
        DataArray::new(Field::new(name, T::get_dtype()).into(), array.arced()).unwrap()
    }
}

impl From<(&str, Box<arrow2::array::Utf8Array<i64>>)> for Utf8Array {
    fn from(item: (&str, Box<arrow2::array::Utf8Array<i64>>)) -> Self {
        let (name, array) = item;
        DataArray::new(Field::new(name, DataType::Utf8).into(), array.arced()).unwrap()
    }
}

impl<T> From<(&str, &[T::Native])> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(item: (&str, &[T::Native])) -> Self {
        let (name, slice) = item;
        let arrow_array = arrow2::array::PrimitiveArray::<T::Native>::from_slice(slice);
        DataArray::new(Field::new(name, T::get_dtype()).into(), arrow_array.arced()).unwrap()
    }
}

impl From<(&str, &[bool])> for BooleanArray {
    fn from(item: (&str, &[bool])) -> Self {
        let (name, slice) = item;
        let arrow_array = arrow2::array::BooleanArray::from_slice(slice);
        DataArray::new(
            Field::new(name, DataType::Boolean).into(),
            arrow_array.arced(),
        )
        .unwrap()
    }
}

impl<T: AsRef<str>> From<(&str, &[T])> for DataArray<Utf8Type> {
    fn from(item: (&str, &[T])) -> Self {
        let (name, slice) = item;
        let arrow_array = arrow2::array::Utf8Array::<i64>::from_slice(slice);
        DataArray::new(Field::new(name, DataType::Utf8).into(), arrow_array.arced()).unwrap()
    }
}

impl<T: DaftDataType> TryFrom<(&str, Box<dyn arrow2::array::Array>)> for DataArray<T> {
    type Error = DaftError;

    fn try_from(item: (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (name, array) = item;
        let self_arrow_type = T::get_dtype().to_arrow().unwrap();
        if !array.data_type().eq(&self_arrow_type) {
            return Err(DaftError::TypeError(format!(
                "mismatch in expected data type {:?} vs {:?}",
                array.data_type(),
                self_arrow_type
            )));
        }
        DataArray::new(
            Field::new(name, array.data_type().into()).into(),
            Arc::from(array),
        )
    }
}
