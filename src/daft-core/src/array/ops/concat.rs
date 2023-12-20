use arrow2::array::Array;

use crate::{array::DataArray, datatypes::DaftPhysicalType};
use common_error::{DaftError, DaftResult};

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;

macro_rules! impl_variable_length_concat {
    ($fn_name:ident, $arrow_type:ty, $create_fn: ident) => {
        fn $fn_name(arrays: &[&dyn arrow2::array::Array]) -> DaftResult<Box<$arrow_type>> {
            let mut num_rows: usize = 0;
            let mut num_bytes: usize = 0;
            let mut need_validity = false;
            for arr in arrays {
                let arr = arr.as_any().downcast_ref::<$arrow_type>().unwrap();

                num_rows += arr.len();
                num_bytes += arr.values().len();
                need_validity |= arr.validity().map(|v| v.unset_bits() > 0).unwrap_or(false);
            }
            let mut offsets = arrow2::offset::Offsets::<i64>::with_capacity(num_rows);

            let mut validity = if need_validity {
                Some(arrow2::bitmap::MutableBitmap::with_capacity(num_rows))
            } else {
                None
            };
            let mut buffer = Vec::<u8>::with_capacity(num_bytes);

            for arr in arrays {
                let arr = arr.as_any().downcast_ref::<$arrow_type>().unwrap();
                offsets.try_extend_from_slice(arr.offsets(), 0, arr.len())?;
                if let Some(ref mut bitmap) = validity {
                    if let Some(b) = arr.validity() {
                        bitmap.extend_from_bitmap(b);
                    } else {
                        bitmap.extend_constant(arr.len(), true);
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
                    validity.map(|v| v.into()),
                )
            }?;
            Ok(Box::new(result_array))
        }
    };
}
impl_variable_length_concat!(
    utf8_concat,
    arrow2::array::Utf8Array<i64>,
    try_new_unchecked
);
impl_variable_length_concat!(binary_concat, arrow2::array::BinaryArray<i64>, try_new);

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
            crate::DataType::Utf8 => {
                let cat_array = utf8_concat(arrow_arrays.as_slice())?;
                DataArray::new(field.clone(), cat_array)
            }
            crate::DataType::Binary => {
                let cat_array = binary_concat(arrow_arrays.as_slice())?;
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
