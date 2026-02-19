use common_error::{DaftError, DaftResult};

#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{array::DataArray, datatypes::DaftPhysicalType};

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
            return Ok(arrays[0].clone());
        }

        let field = &arrays[0].field;

        let arrow_arrays: Vec<_> = arrays.iter().map(|s| s.to_arrow()).collect();
        let arrow_refs: Vec<&dyn arrow::array::Array> =
            arrow_arrays.iter().map(|a| a.as_ref()).collect();
        let cat_array = arrow::compute::concat(&arrow_refs)?;
        Self::from_arrow(field.clone(), cat_array)
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        use std::sync::Arc;

        use arrow::array::NullBufferBuilder;

        use crate::datatypes::python::PythonBuffer;

        if arrays.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 array to perform concat".to_string(),
            ));
        }

        if arrays.len() == 1 {
            return Ok(arrays[0].clone());
        }

        let field = Arc::new(arrays[0].field().clone());

        let nulls = if arrays.iter().any(|a| a.nulls().is_some()) {
            let total_len = arrays.iter().map(|a| a.len()).sum();

            let mut null_builder = NullBufferBuilder::new(total_len);

            for a in arrays {
                if let Some(v) = a.nulls() {
                    for b in v {
                        null_builder.append(b); // TODO: Replace with .append_buffer in v57.1.0
                    }
                } else {
                    null_builder.append_n_non_nulls(a.len());
                }
            }

            null_builder.finish()
        } else {
            None
        };

        let values: PythonBuffer = arrays
            .iter()
            .flat_map(|a| a.values().iter().cloned())
            .collect();

        Ok(Self::new(field, values, nulls))
    }
}
