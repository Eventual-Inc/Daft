use arrow2::{array::{Array, growable::GrowableUtf8}, offset};

use crate::{array::DataArray, datatypes::{DaftPhysicalType, Utf8Array}};
use common_error::{DaftError, DaftResult};

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;

use super::as_arrow::AsArrow;



fn utf8_concat(arrays: &[&arrow2::array::Utf8Array<i64>]) -> DaftResult<Box<arrow2::array::Utf8Array<i64>>> {
    let mut num_rows: usize = 0;
    let mut num_bytes: usize = 0;
    let mut need_validity = false;
    for arr in arrays {
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
        offsets.try_extend_from_slice(arr.offsets(), 0, arr.len()).unwrap();
        if let Some(ref mut bitmap) = validity {
            if let Some(b) = arr.validity() {
                bitmap.extend_from_bitmap(b);
            } else {
                bitmap.extend_constant(arr.len(), true);
            }
        }
        buffer.extend_from_slice(arr.values().as_slice())
    }
    let result_array = unsafe {
        arrow2::array::Utf8Array::<i64>::try_new_unchecked(arrow2::datatypes::DataType::LargeUtf8, offsets.into(), buffer.into(), validity.map(|v| v.into()))
    }?;
    Ok(Box::new(result_array))

}


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
                let utf8arrays = arrow_arrays
                .iter()
                .map(|s| {
                    s.as_any()
                        .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
                let cat_array = utf8_concat(utf8arrays.as_slice())?;
                DataArray::new(field.clone(), cat_array)
            },
            _ => {
                let cat_array: Box<dyn Array> =
                    arrow2::compute::concatenate::concatenate(arrow_arrays.as_slice())?;
                DataArray::try_from((field.clone(), cat_array))
            }
        }
    }
}
