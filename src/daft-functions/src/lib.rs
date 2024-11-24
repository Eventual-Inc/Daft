#![feature(async_closure)]
pub mod count_matches;
pub mod distance;
pub mod float;
pub mod hash;
pub mod image;
pub mod list;
pub mod minhash;
pub mod numeric;
pub mod temporal;
pub mod to_struct;
pub mod tokenize;
pub mod uri;
pub mod utf8;

use common_error::DaftError;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use snafu::Snafu;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    // keep in sorted order
    parent.add_function(wrap_pyfunction_bound!(
        count_matches::python::utf8_count_matches,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(
        distance::cosine::python::cosine_distance,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(hash::python::hash, parent)?)?;

    parent.add_function(wrap_pyfunction_bound!(minhash::python::minhash, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        to_struct::python::to_struct,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(
        tokenize::python::tokenize_decode,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(
        tokenize::python::tokenize_encode,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(uri::python::url_download, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(uri::python::url_upload, parent)?)?;
    numeric::register_modules(parent)?;
    image::register_modules(parent)?;
    float::register_modules(parent)?;
    temporal::register_modules(parent)?;
    list::register_modules(parent)?;
    utf8::register_modules(parent)?;
    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        Self::new(std::io::ErrorKind::Other, err)
    }
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        Self::External(err.into())
    }
}
