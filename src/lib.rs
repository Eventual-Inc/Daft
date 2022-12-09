mod ffi;

use pyo3::prelude::*;
use arrow2::compute::arithmetics::basic::add_scalar;
use arrow2::array::PrimitiveArray;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn add_1(array: &PyAny, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let rarray = ffi::array_to_rust(array);
    let added = add_scalar(rarray?.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap(), &10);
    ffi::to_py_array(Box::new(added), py, pyarrow)
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn daft_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add_1, m)?)?;
    Ok(())
}
