mod ffi;
mod hashing;
mod search_sorted;

use std::iter::zip;

use arrow2::array::{Array, PrimitiveArray};
use arrow2::datatypes::DataType;

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
    let rseed: Option<Box<dyn Array>> = match seed {
        Some(seed) => Some(ffi::array_to_rust(seed)?),
        None => None,
    };
    let hashed;
    if rseed.is_some() {
        let rseed_val = rseed.unwrap();
        assert_eq!(rseed_val.len(), rarray.len());
        assert_eq!(*rseed_val.data_type(), DataType::UInt64);
        let downcasted_seed = rseed_val
            .as_ref()
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        hashed = hashing::hash(rarray.as_ref(), Some(downcasted_seed));
    } else {
        hashed = hashing::hash(rarray.as_ref(), None);
    }

    ffi::to_py_array(Box::new(hashed.unwrap()), py, pyarrow)
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
    let result_idx =
        search_sorted::search_sorted(rsorted_array.as_ref(), rkeys_array.as_ref(), input_reversed);

    ffi::to_py_array(Box::new(result_idx.unwrap()), py, pyarrow)
}

#[pyfunction]
fn search_sorted_multiple_pyarrow_array(
    sorted_arrays: &PyList,
    key_arrays: &PyList,
    descending_array: Vec<bool>,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    assert_eq!(sorted_arrays.len(), key_arrays.len());
    assert_eq!(sorted_arrays.len(), descending_array.len());
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

    let result_idx = search_sorted::search_sorted_multi_array(
        &rsorted_arrays_refs,
        &key_arrays_refs,
        &descending_array,
    );

    ffi::to_py_array(Box::new(result_idx.unwrap()), py, pyarrow)
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn daft_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hash_pyarrow_array, m)?)?;
    m.add_function(wrap_pyfunction!(search_sorted_pyarrow_array, m)?)?;
    m.add_function(wrap_pyfunction!(search_sorted_multiple_pyarrow_array, m)?)?;

    Ok(())
}
