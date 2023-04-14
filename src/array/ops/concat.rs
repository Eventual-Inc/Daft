use arrow2::array::Array;

use crate::{
    array::{vec_backed::VecBackedArray, DataArray},
    datatypes::DaftDataType,
    error::{DaftError, DaftResult},
};

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 array to perform concat".to_string(),
            ));
        }

        if arrays.len() == 1 {
            return Ok((*arrays.first().unwrap()).clone());
        }
        let dtype = arrays.first().unwrap().data_type();

        let arrow_arrays: Vec<_> = arrays.iter().map(|s| s.data.as_ref()).collect();
        let cat_array: Box<dyn Array> = match dtype {
            #[cfg(feature = "python")]
            crate::datatypes::DataType::Python => {
                use pyo3::prelude::*;

                Box::new(VecBackedArray::concatenate(
                    arrow_arrays
                        .iter()
                        .map(|s| {
                            s.as_any()
                                .downcast_ref::<VecBackedArray<PyObject>>()
                                .unwrap()
                        })
                        .collect(),
                ))
            }
            _ => arrow2::compute::concatenate::concatenate(arrow_arrays.as_slice())?,
        };
        let name = arrays.first().unwrap().name();
        DataArray::try_from((name, cat_array))
    }
}
