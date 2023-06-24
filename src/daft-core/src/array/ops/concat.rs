use arrow2::array::Array;

use crate::{array::DataArray, datatypes::DaftPhysicalType};
use common_error::{DaftError, DaftResult};

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
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

        let field = &arrays.first().unwrap().field;

        let arrow_arrays: Vec<_> = arrays.iter().map(|s| s.data.as_ref()).collect();
        match field.dtype {
            #[cfg(feature = "python")]
            crate::datatypes::DataType::Python => {
                use pyo3::prelude::*;

                let cat_array = Box::new(PseudoArrowArray::concatenate(
                    arrow_arrays
                        .iter()
                        .map(|s| {
                            s.as_any()
                                .downcast_ref::<PseudoArrowArray<PyObject>>()
                                .unwrap()
                        })
                        .collect(),
                ));
                DataArray::new(field.clone(), cat_array)
            }
            _ => {
                let cat_array: Box<dyn Array> =
                    arrow2::compute::concatenate::concatenate(arrow_arrays.as_slice())?;
                DataArray::try_from((field.clone(), cat_array))
            }
        }
    }
}
