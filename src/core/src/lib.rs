#![feature(hash_raw_entry)]
#![feature(async_closure)]
#[macro_use]
extern crate lazy_static;

mod array;
mod datatypes;
mod dsl;
use common_error;
mod io;
mod kernels;
mod schema;
mod series;
mod table;
mod utils;

use common_error::DaftError;

#[cfg(feature = "python")]
mod ffi;
#[cfg(feature = "python")]
mod python;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUILD_TYPE_DEV: &str = "dev";
const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("RUST_DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};

#[cfg(feature = "python")]
pub mod pylib {

    use super::python;
    use super::{DAFT_BUILD_TYPE, VERSION};
    use pyo3::prelude::*;

    #[pyfunction]
    fn version() -> &'static str {
        VERSION
    }

    #[pyfunction]
    fn build_type() -> &'static str {
        DAFT_BUILD_TYPE
    }

    #[pymodule]
    fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
        pyo3_log::init();

        python::register_modules(_py, m)?;
        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(build_type))?;
        Ok(())
    }
}


impl From<arrow2::error::Error> for DaftError {
    fn from(error: arrow2::error::Error) -> Self {
        DaftError::ArrowError(error.to_string())
    }
}

#[cfg(feature = "python")]
impl From<pyo3::PyErr> for DaftError {
    fn from(error: pyo3::PyErr) -> Self {
        DaftError::PyO3Error(error)
    }
}

impl From<serde_json::Error> for DaftError {
    fn from(error: serde_json::Error) -> Self {
        DaftError::IoError(error.into())
    }
}

impl From<io::Error> for DaftError {
    fn from(error: io::Error) -> Self {
        DaftError::IoError(error)
    }
}