use std::sync::Arc;

use arrow2::array::{new_empty_array, new_null_array};

use crate::{
    array::DataArray,
    datatypes::{DaftDataType, Field},
};

impl<T> DataArray<T>
where
    T: DaftDataType,
{
    /// Creates a DataArray<T> of size `length` that is filled with all nulls.
    pub fn full_null(name: &str, length: usize) -> Self {
        if !T::get_dtype().is_arrow() {
            panic!("Only arrow types are supported for null arrays");
        }
        let arr = new_null_array(T::get_dtype().to_arrow().unwrap(), length);

        DataArray::new(Arc::new(Field::new(name, T::get_dtype())), arr).unwrap()
    }

    pub fn empty(name: &str) -> Self {
        if !T::get_dtype().is_arrow() {
            panic!("Only arrow types are supported for empty arrays");
        }
        let arr = new_empty_array(T::get_dtype().to_arrow().unwrap());

        DataArray::new(Arc::new(Field::new(name, T::get_dtype())), arr).unwrap()
    }
}
