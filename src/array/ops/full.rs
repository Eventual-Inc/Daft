use std::sync::Arc;

use arrow2::array::new_null_array;

use crate::{
    array::data_array::DataArray,
    datatypes::{DaftDataType, Field},
};

impl<T> DataArray<T>
where
    T: DaftDataType,
{
    /// Creates a DataArray<T> of size `length` that is filled with all nulls.
    pub fn full_null(length: usize) -> Self {
        if !T::get_dtype().is_arrow() {
            panic!("Only arrow types are supported for null arrays");
        }
        let arr = new_null_array(T::get_dtype().to_arrow().unwrap(), length);

        DataArray::new(
            Arc::new(Field::new("null_array", T::get_dtype())),
            Arc::from(arr),
        )
        .unwrap()
    }
}
