#![allow(dead_code)]

mod array;
mod datatypes;
mod dsl;
mod error;
mod ffi;
mod kernels;
mod schema;
mod series;
mod table;
mod utils;

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
