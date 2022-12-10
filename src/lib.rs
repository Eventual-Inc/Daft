mod ffi;
mod hashing;
mod search_sorted;

use std::iter::zip;

use arrow2::array::{Array, PrimitiveArray};
use arrow2::datatypes::DataType;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn hash_pyarrow_array(
    pyarray: &PyAny,
    py: Python,
    pyarrow: &PyModule,
    seed: Option<&PyAny>,
) -> PyResult<PyObject> {
    let rarray = ffi::array_to_rust(pyarray)?;
    let hashed;
    if let Some(seed) = seed {
        let rseed = ffi::array_to_rust(seed)?;
        if rseed.len() != rarray.len() {
            return Err(PyValueError::new_err(format!(
                "seed length does not match array length: {} vs {}",
                rseed.len(),
                rarray.len()
            )));
        }
        if *rseed.data_type() != DataType::UInt64 {
            return Err(PyValueError::new_err(format!(
                "seed data type expected to be UInt64, got {:?}",
                *rseed.data_type()
            )));
        }

        let downcasted_seed = rseed
            .as_ref()
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        hashed = py.allow_threads(move || hashing::hash(rarray.as_ref(), Some(downcasted_seed)));
    } else {
        hashed = py.allow_threads(move || hashing::hash(rarray.as_ref(), None));
    }
    match hashed {
        Err(e) => return Err(PyValueError::new_err(e.to_string())),
        Ok(s) => return ffi::to_py_array(Box::new(s), py, pyarrow),
    }
}

#[pyfunction]
fn search_sorted_pyarrow_array(
    sorted_array: &PyAny,
    keys: &PyAny,
    input_reversed: bool,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let rsorted_array = ffi::array_to_rust(sorted_array)?;
    let rkeys_array = ffi::array_to_rust(keys)?;
    let result_idx = py.allow_threads(move || {
        search_sorted::search_sorted(rsorted_array.as_ref(), rkeys_array.as_ref(), input_reversed)
    });

    match result_idx {
        Err(e) => return Err(PyValueError::new_err(e.to_string())),
        Ok(s) => return ffi::to_py_array(Box::new(s), py, pyarrow),
    }
}

#[pyfunction]
fn search_sorted_multiple_pyarrow_array(
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
        search_sorted::search_sorted_multi_array(
            &rsorted_arrays_refs,
            &key_arrays_refs,
            &descending_array,
        )
    });
    match result_idx {
        Err(e) => return Err(PyValueError::new_err(e.to_string())),
        Ok(s) => return ffi::to_py_array(Box::new(s), py, pyarrow),
    }
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hash_pyarrow_array, m)?)?;
    m.add_function(wrap_pyfunction!(search_sorted_pyarrow_array, m)?)?;
    m.add_function(wrap_pyfunction!(search_sorted_multiple_pyarrow_array, m)?)?;

    Ok(())
}
