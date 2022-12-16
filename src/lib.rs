mod ffi;
mod kernels;

use pyo3::prelude::*;

#[pymodule]
fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    kernels::register_kernels(_py, m)?;
    Ok(())
}
