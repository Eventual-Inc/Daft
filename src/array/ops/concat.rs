use arrow2::array::Array;

use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{DaftArrowBackedType, DaftDataType, DaftPhysicalType, DataType},
    error::{DaftError, DaftResult},
};

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
        let name = arrays.first().unwrap().name();

        let arrow_arrays: Vec<_> = arrays.iter().map(|s| s.data.as_ref()).collect();
        let dtype = arrays.first().unwrap().data_type();
        match dtype {
            #[cfg(feature = "python")]
            crate::datatypes::DataType::Python => {
                use crate::datatypes::Field;
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
                let field = Field::new(name, DataType::Python);
                DataArray::new(field.into(), cat_array)
            }
            _ => {
                let cat_array: Box<dyn Array> =
                    arrow2::compute::concatenate::concatenate(arrow_arrays.as_slice())?;
                DataArray::try_from((name, cat_array))
            }
        }
    }
}
