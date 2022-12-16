use std::{cmp::Ordering, iter::zip};

use arrow2::{
    array::ord::build_compare,
    array::Array,
    array::{ord::DynComparator, PrimitiveArray, Utf8Array},
    datatypes::{DataType, PhysicalType},
    error::{Error, Result},
    types::{NativeType, Offset},
};

use crate::ffi;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

fn search_sorted_primitive_array<T: NativeType + PartialOrd>(
    sorted_array: &PrimitiveArray<T>,
    keys: &PrimitiveArray<T>,
    input_reversed: bool,
) -> PrimitiveArray<u64> {
    let array_size = sorted_array.len();

    let mut left = 0_usize;
    let mut right = array_size;

    let mut results: Vec<u64> = Vec::with_capacity(array_size);

    let mut last_key = keys.iter().next().unwrap_or(None);

    for key_val in keys.iter() {
        let is_last_key_le = match (last_key, key_val) {
            (None, None) => false,
            (Some(last_key), Some(key_val)) => last_key.le(key_val),
            (None, _) => false,
            (_, None) => true,
        };
        if is_last_key_le {
            right = array_size;
        } else {
            left = 0;
            right = if right < array_size {
                right + 1
            } else {
                array_size
            };
        }
        while left < right {
            let mid_idx = left + ((right - left) >> 1);
            let corrected_idx = if input_reversed {
                array_size - mid_idx - 1
            } else {
                mid_idx
            };
            let mid_val = unsafe { sorted_array.value_unchecked(corrected_idx) };
            let is_key_val_le = match (key_val, sorted_array.is_valid(corrected_idx)) {
                (None, true) => false,
                (None, false) => true,
                (Some(key_val), true) => key_val.le(&mid_val),
                (_, false) => true,
            };
            if is_key_val_le {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        let result_idx = if input_reversed {
            array_size - left
        } else {
            left
        };
        results.push(result_idx.try_into().unwrap());
        last_key = key_val;
    }

    PrimitiveArray::<u64>::new(DataType::UInt64, results.into(), None)
}

fn search_sorted_utf_array<O: Offset>(
    sorted_array: &Utf8Array<O>,
    keys: &Utf8Array<O>,
    input_reversed: bool,
) -> PrimitiveArray<u64> {
    let array_size = sorted_array.len();
    let mut left = 0_usize;
    let mut right = array_size;

    let mut results: Vec<u64> = Vec::with_capacity(array_size);
    let mut last_key = keys.iter().next().unwrap_or(None);
    for key_val in keys.iter() {
        let is_last_key_le = match (last_key, key_val) {
            (None, None) => false,
            (Some(last_key), Some(key_val)) => last_key.le(key_val),
            (None, _) => false,
            (_, None) => true,
        };
        if is_last_key_le {
            right = array_size;
        } else {
            left = 0;
            right = if right < array_size {
                right + 1
            } else {
                array_size
            };
        }
        while left < right {
            let mid_idx = left + ((right - left) >> 1);
            let corrected_idx = if input_reversed {
                array_size - mid_idx - 1
            } else {
                mid_idx
            };
            let mid_val = unsafe { sorted_array.value_unchecked(corrected_idx) };
            let is_key_val_le = match (key_val, sorted_array.is_valid(corrected_idx)) {
                (None, true) => false,
                (None, false) => true,
                (Some(key_val), true) => key_val.le(mid_val),
                (_, false) => true,
            };
            if is_key_val_le {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        let result_idx = if input_reversed {
            array_size - left
        } else {
            left
        };
        results.push(result_idx.try_into().unwrap());
        last_key = key_val;
    }

    PrimitiveArray::<u64>::new(DataType::UInt64, results.into(), None)
}

macro_rules! with_match_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use arrow2::datatypes::PrimitiveType::*;
    // use arrow2::types::{days_ms, months_days_ns};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        // DaysMs => __with_ty__! { days_ms },
        // MonthDayNano => __with_ty__! { months_days_ns },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
        _ => return Err(Error::NotYetImplemented(format!(
            "search_sorted not implemented for type {:?}",
            $key_type
        )))
    }
})}

type IsValid = Box<dyn Fn(usize) -> bool + Send + Sync>;
fn build_is_valid(array: &dyn Array) -> IsValid {
    if let Some(validity) = array.validity() {
        let validity = validity.clone();
        Box::new(move |x| unsafe { validity.get_bit_unchecked(x) })
    } else {
        Box::new(move |_| true)
    }
}

fn build_compare_with_nulls(
    left: &dyn Array,
    right: &dyn Array,
    reversed: bool,
) -> Result<DynComparator> {
    let comparator = build_compare(left, right)?;
    let left_is_valid = build_is_valid(left);
    let right_is_valid = build_is_valid(right);
    if reversed {
        Ok(Box::new(move |i: usize, j: usize| {
            match (left_is_valid(i), right_is_valid(j)) {
                (true, true) => comparator(i, j).reverse(),
                (false, true) => Ordering::Less,
                (false, false) => Ordering::Equal,
                (true, false) => Ordering::Greater,
            }
        }))
    } else {
        Ok(Box::new(move |i: usize, j: usize| {
            match (left_is_valid(i), right_is_valid(j)) {
                (true, true) => comparator(i, j),
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
                (true, false) => Ordering::Less,
            }
        }))
    }
}

pub fn search_sorted_multi_array(
    sorted_arrays: &Vec<&dyn Array>,
    key_arrays: &Vec<&dyn Array>,
    input_reversed: &Vec<bool>,
) -> Result<PrimitiveArray<u64>> {
    if sorted_arrays.is_empty() || key_arrays.is_empty() {
        return Err(Error::InvalidArgumentError(
            "Passed in empty number of columns".to_string(),
        ));
    }

    if sorted_arrays.len() != key_arrays.len() {
        return Err(Error::InvalidArgumentError(
            "Mismatch in number of columns".to_string(),
        ));
    }

    let sorted_array_size = sorted_arrays[0].len();
    for sorted_arr in sorted_arrays {
        if sorted_arr.len() != sorted_array_size {
            return Err(Error::InvalidArgumentError(format!(
                "Mismatch in number of rows: {} vs {}",
                sorted_arr.len(),
                sorted_array_size
            )));
        }
    }
    let key_array_size = key_arrays[0].len();
    for key_arr in key_arrays {
        if key_arr.len() != key_array_size {
            return Err(Error::InvalidArgumentError(format!(
                "Mismatch in number of rows: {} vs {}",
                key_arr.len(),
                sorted_array_size
            )));
        }
    }
    let mut cmp_list = Vec::with_capacity(sorted_arrays.len());
    for ((sorted_arr, key_arr), reversed) in zip(sorted_arrays, key_arrays).zip(input_reversed) {
        cmp_list.push(build_compare_with_nulls(*sorted_arr, *key_arr, *reversed)?);
    }

    let combined_comparator = |a_idx: usize, b_idx: usize| -> Ordering {
        for comparator in cmp_list.iter() {
            match comparator(a_idx, b_idx) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    };
    let mut results: Vec<u64> = Vec::with_capacity(key_array_size);

    for key_idx in 0..key_array_size {
        let mut left = 0;
        let mut right = sorted_array_size;
        while left < right {
            let mid_idx = left + ((right - left) >> 1);
            if combined_comparator(mid_idx, key_idx).is_ge() {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        results.push(left.try_into().unwrap());
    }
    Ok(PrimitiveArray::<u64>::new(
        DataType::UInt64,
        results.into(),
        None,
    ))
}

pub fn search_sorted(
    sorted_array: &dyn Array,
    keys: &dyn Array,
    input_reversed: bool,
) -> Result<PrimitiveArray<u64>> {
    use PhysicalType::*;
    if sorted_array.data_type() != keys.data_type() {
        let error_string = format!(
            "sorted array data type does not match keys data type: {:?} vs {:?}",
            sorted_array.data_type(),
            keys.data_type()
        );
        return Err(Error::InvalidArgumentError(error_string));
    }
    Ok(match sorted_array.data_type().to_physical_type() {
        // Boolean => hash_boolean(array.as_any().downcast_ref().unwrap()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            search_sorted_primitive_array::<$T>(sorted_array.as_any().downcast_ref().unwrap(), keys.as_any().downcast_ref().unwrap(), input_reversed)
        }),
        Utf8 => search_sorted_utf_array::<i32>(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        ),
        LargeUtf8 => search_sorted_utf_array::<i64>(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        ),
        t => {
            return Err(Error::NotYetImplemented(format!(
                "search_sorted not implemented for type {:?}",
                t
            )))
        }
    })
}

#[pyfunction]
pub fn search_sorted_pyarrow_array(
    sorted_array: &PyAny,
    keys: &PyAny,
    input_reversed: bool,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let rsorted_array = ffi::array_to_rust(sorted_array)?;
    let rkeys_array = ffi::array_to_rust(keys)?;
    let result_idx = py.allow_threads(move || {
        search_sorted(rsorted_array.as_ref(), rkeys_array.as_ref(), input_reversed)
    });

    match result_idx {
        Err(e) => Err(PyValueError::new_err(e.to_string())),
        Ok(s) => ffi::to_py_array(Box::new(s), py, pyarrow),
    }
}

#[pyfunction]
pub fn search_sorted_multiple_pyarrow_array(
    sorted_arrays: &PyList,
    key_arrays: &PyList,
    descending_array: Vec<bool>,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    if sorted_arrays.len() != key_arrays.len() {
        return Err(PyValueError::new_err(
            "number of columns for sorted arrays and key arrays does not match",
        ));
    }
    if sorted_arrays.len() != descending_array.len() {
        return Err(PyValueError::new_err(
            "number of columns for sorted arrays and descending_array does not match",
        ));
    }
    let mut rsorted_arrays: Vec<Box<dyn Array>> = Vec::with_capacity(sorted_arrays.len());
    let mut rkeys_arrays: Vec<Box<dyn Array>> = Vec::with_capacity(key_arrays.len());

    for (sorted_arr, key_arr) in zip(sorted_arrays.iter(), key_arrays.iter()) {
        rsorted_arrays.push(ffi::array_to_rust(sorted_arr)?);
        rkeys_arrays.push(ffi::array_to_rust(key_arr)?);
    }

    let rsorted_arrays_refs = rsorted_arrays
        .iter()
        .map(Box::as_ref)
        .collect::<Vec<&dyn Array>>();
    let key_arrays_refs = rkeys_arrays
        .iter()
        .map(Box::as_ref)
        .collect::<Vec<&dyn Array>>();

    let result_idx = py.allow_threads(move || {
        search_sorted_multi_array(&rsorted_arrays_refs, &key_arrays_refs, &descending_array)
    });
    match result_idx {
        Err(e) => Err(PyValueError::new_err(e.to_string())),
        Ok(s) => ffi::to_py_array(Box::new(s), py, pyarrow),
    }
}
