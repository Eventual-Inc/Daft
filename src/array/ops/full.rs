use arrow2::array::{new_null_array, PrimitiveArray};

use crate::{array::data_array::DataArray, datatypes::DaftNumericType};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    /// Creates a DataArray<T> of size `length` that is filled with all nulls.
    pub fn full_null(length: usize) -> Self {
        let arr = new_null_array(T::get_dtype().to_arrow().unwrap(), length);
        let downcasted = arr
            .as_any()
            .downcast_ref::<Box<PrimitiveArray<T::Native>>>()
            .unwrap();
        DataArray::from(downcasted.to_owned())
    }
}
