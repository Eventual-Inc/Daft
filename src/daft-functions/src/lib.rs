pub mod hash;
pub mod uri;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(uri::python::url_upload))?;
    parent.add_wrapped(wrap_pyfunction!(uri::python::url_download))?;
    parent.add_wrapped(wrap_pyfunction!(hash::python::hash))?;

    Ok(())
}
