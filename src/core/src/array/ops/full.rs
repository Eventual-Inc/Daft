use std::sync::Arc;

#[cfg(feature = "python")]
use pyo3::Python;

use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{DaftPhysicalType, DataType, Field},
};

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
{
    /// Creates a DataArray<T> of size `length` that is filled with all nulls.
    pub fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let field = Field::new(name, dtype.clone());
        #[cfg(feature = "python")]
        if dtype.is_python() {
            let py_none = Python::with_gil(|py: Python| py.None());

            return DataArray::new(
                field.into(),
                Box::new(PseudoArrowArray::from_pyobj_vec(vec![py_none; length])),
            )
            .unwrap();
        }

        let arrow_dtype = dtype.to_arrow();
        match arrow_dtype {
            Ok(arrow_dtype) => DataArray::<T>::new(
                Arc::new(Field::new(name.to_string(), dtype.clone())),
                arrow2::array::new_null_array(arrow_dtype, length),
            )
            .unwrap(),
            Err(e) => panic!("Cannot create DataArray from non-arrow dtype: {e}"),
        }
    }

    pub fn empty(name: &str, dtype: &DataType) -> Self {
        let field = Field::new(name, dtype.clone());
        #[cfg(feature = "python")]
        if dtype.is_python() {
            return DataArray::new(
                field.into(),
                Box::new(PseudoArrowArray::from_pyobj_vec(vec![])),
            )
            .unwrap();
        }

        let arrow_dtype = dtype.to_arrow();
        match arrow_dtype {
            Ok(arrow_dtype) => DataArray::<T>::new(
                Arc::new(Field::new(name.to_string(), dtype.clone())),
                arrow2::array::new_empty_array(arrow_dtype),
            )
            .unwrap(),
            Err(e) => panic!("Cannot create DataArray from non-arrow dtype: {e}"),
        }
    }
}
