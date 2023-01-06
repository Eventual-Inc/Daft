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

const BUILD_TYPE_DEV: &str = "dev";
const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};

#[pyfunction]
fn build_type() -> &'static str {
    DAFT_BUILD_TYPE
}

#[pymodule]
fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    kernels::register_kernels(_py, m)?;
    m.add_wrapped(wrap_pyfunction!(version))?;
    m.add_wrapped(wrap_pyfunction!(build_type))?;
    Ok(())
}
