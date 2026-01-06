use std::sync::Arc;

use arrow::array::ArrayRef;
use common_error::{DaftError, DaftResult};

#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, ops::from_arrow::FromArrow},
    datatypes::DaftPhysicalType,
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

        let field = &arrays.first().unwrap().field;

        let arrow_arrays: Vec<ArrayRef> = arrays.iter().map(|s| s.to_arrow()).collect();

        let array_refs = arrow_arrays
            .iter()
            .map(|arr| arr.as_ref())
            .collect::<Vec<_>>();
        let cat_array = arrow::compute::concat(array_refs.as_slice())?;
        Self::from_arrow(field.clone(), cat_array)
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        use daft_arrow::buffer::{Buffer, NullBufferBuilder};
        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 array to perform concat".to_string(),
            ));
        }

        if arrays.len() == 1 {
            return Ok((*arrays.first().unwrap()).clone());
        }

        let field = Arc::new(arrays.first().unwrap().field().clone());

        let validity = if arrays.iter().any(|a| a.validity().is_some()) {
            let total_len = arrays.iter().map(|a| a.len()).sum();

            let mut validity = NullBufferBuilder::new(total_len);

            for a in arrays {
                if let Some(v) = a.validity() {
                    for b in v {
                        validity.append(b); // TODO: Replace with .append_buffer in v57.1.0
                    }
                } else {
                    validity.append_n_non_nulls(a.len());
                }
            }

            validity.finish()
        } else {
            None
        };

        let values = Buffer::from_iter(arrays.iter().flat_map(|a| a.values().iter().cloned()));

        Ok(Self::new(field, values, validity))
    }
}
