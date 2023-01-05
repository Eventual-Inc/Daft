use arrow2::array::new_null_array;

use crate::{array::data_array::DataArray, datatypes::DaftNumericType};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn full_null(length: usize) -> Self {
        let arr = new_null_array(T::get_dtype().to_arrow().unwrap(), length);
        DataArray::from(arr)
    }
}
