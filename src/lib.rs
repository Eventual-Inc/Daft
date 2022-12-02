mod ffi;
mod hashing;

use arrow2::array::{Array, PrimitiveArray};
use arrow2::datatypes::DataType;

use arrow2::compute::arithmetics::basic::add_scalar;
use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn add_1(array: &PyAny, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let rarray = ffi::array_to_rust(array);
    let added = add_scalar(
        rarray?
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap(),
        &10,
    );
    ffi::to_py_array(Box::new(added), py, pyarrow)
}

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

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn daft_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add_1, m)?)?;
    m.add_function(wrap_pyfunction!(hash_pyarrow_array, m)?)?;

    Ok(())
}
