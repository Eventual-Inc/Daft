use std::sync::Arc;

use crate::{
    array::DataArray,
    datatypes::{DaftDataType, DataType, Field},
};

impl<T> DataArray<T>
where
    T: DaftDataType,
{
    /// Creates a DataArray<T> of size `length` that is filled with all nulls.
    pub fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let arrow_dtype = dtype.to_arrow();
        match arrow_dtype {
            Ok(arrow_dtype) => DataArray::<T>::new(
                Arc::new(Field {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                }),
                arrow2::array::new_null_array(arrow_dtype, length),
            )
            .unwrap(),
            Err(e) => panic!("Cannot create DataArray from non-arrow dtype: {e}"),
        }
    }

    pub fn empty(name: &str, dtype: &DataType) -> Self {
        let arrow_dtype = dtype.to_arrow();
        match arrow_dtype {
            Ok(arrow_dtype) => DataArray::<T>::new(
                Arc::new(Field {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                }),
                arrow2::array::new_empty_array(arrow_dtype),
            )
            .unwrap(),
            Err(e) => panic!("Cannot create DataArray from non-arrow dtype: {e}"),
        }
    }
}
