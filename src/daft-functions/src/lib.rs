#![feature(async_closure)]
pub mod distance;
pub mod hash;
pub mod minhash;
pub mod uri;

use common_error::DaftError;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use snafu::Snafu;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(uri::python::url_upload))?;
    parent.add_wrapped(wrap_pyfunction!(uri::python::url_download))?;
    parent.add_wrapped(wrap_pyfunction!(hash::python::hash))?;
    parent.add_wrapped(wrap_pyfunction!(distance::cosine::python::cosine))?;
    parent.add_wrapped(wrap_pyfunction!(minhash::python::minhash))?;

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err)
    }
}
impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        DaftError::External(err.into())
    }
}
