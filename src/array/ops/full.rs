use std::sync::Arc;

use arrow2::array::{new_null_array, PrimitiveArray};

use crate::{
    array::data_array::DataArray,
    datatypes::{DaftDataType, DaftNumericType, Field, NullArray, NullType},
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

        // DataArray::from(downcasted.clone().boxed())
        DataArray::new(
            Arc::new(Field::new("null_array", T::get_dtype())),
            Arc::from(arr),
        )
        .unwrap()
    }
}

// impl NullArray {
//     /// Creates a DataArray<T> of size `length` that is filled with all nulls.
//     pub fn full_null(length: usize) -> Self {
//         let arr = new_null_array(arrow2::datatypes::DataType::Null, length);
//         let downcasted = arr
//             .as_any()
//             .downcast_ref::<PrimitiveArray<T::Native>>()
//             .unwrap();

//         DataArray::from(downcasted.clone().boxed())
//     }
// }
