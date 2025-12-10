use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_arrow::array::Array;

#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{array::DataArray, datatypes::DaftPhysicalType};

macro_rules! impl_variable_length_concat {
    ($fn_name:ident, $arrow_type:ty, $create_fn: ident) => {
        fn $fn_name(arrays: &[&dyn daft_arrow::array::Array]) -> DaftResult<Box<$arrow_type>> {
            let mut num_rows: usize = 0;
            let mut num_bytes: usize = 0;
            let mut need_validity = false;
            for arr in arrays {
                let arr = arr.as_any().downcast_ref::<$arrow_type>().unwrap();

                num_rows += arr.len();
                num_bytes += arr.values().len();
                need_validity |= arr.validity().map(|v| v.unset_bits() > 0).unwrap_or(false);
            }
            let mut offsets = daft_arrow::offset::Offsets::<i64>::with_capacity(num_rows);

            let mut validity = if need_validity {
                Some(daft_arrow::buffer::NullBufferBuilder::new(num_rows))
            } else {
                None
            };
            let mut buffer = Vec::<u8>::with_capacity(num_bytes);

            for arr in arrays {
                let arr = arr.as_any().downcast_ref::<$arrow_type>().unwrap();
                offsets.try_extend_from_slice(arr.offsets(), 0, arr.len())?;
                if let Some(ref mut bitmap) = validity {
                    if let Some(v) = arr.validity() {
                        for b in v.iter() {
                            // TODO: Replace with .append_buffer in v57.1.0
                            bitmap.append(b);
                        }
                    } else {
                        bitmap.append_n_non_nulls(arr.len());
                    }
                }
                let range = (*arr.offsets().first() as usize)..(*arr.offsets().last() as usize);
                buffer.extend_from_slice(&arr.values().as_slice()[range]);
            }
            let dtype = arrays.first().unwrap().data_type().clone();
            #[allow(unused_unsafe)]
            let result_array = unsafe {
                <$arrow_type>::$create_fn(
                    dtype,
                    offsets.into(),
                    buffer.into(),
                    daft_arrow::buffer::wrap_null_buffer(
                        validity.map(|mut v| v.finish()).flatten(),
                    ),
                )
            }?;
            Ok(Box::new(result_array))
        }
    };
}
impl_variable_length_concat!(
    utf8_concat,
    daft_arrow::array::Utf8Array<i64>,
    try_new_unchecked
);
impl_variable_length_concat!(binary_concat, daft_arrow::array::BinaryArray<i64>, try_new);

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
            crate::datatypes::DataType::Utf8 => {
                let cat_array = utf8_concat(arrow_arrays.as_slice())?;
                Self::new(field.clone(), cat_array)
            }
            crate::datatypes::DataType::Binary => {
                let cat_array = binary_concat(arrow_arrays.as_slice())?;
                Self::new(field.clone(), cat_array)
            }
            _ => {
                let cat_array: Box<dyn Array> =
                    daft_arrow::compute::concatenate::concatenate(arrow_arrays.as_slice())?;
                Self::try_from((field.clone(), cat_array))
            }
        }
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
