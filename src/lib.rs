mod ffi;
mod kernels;

use pyo3::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");
#[pyfunction]
fn version() -> &'static str {
    VERSION
}

#[pymodule]
fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    kernels::register_kernels(_py, m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    Ok(())
}
