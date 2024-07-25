pub mod catalog;
pub mod parser;
#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<python::PyCatalog>()?;
    parent.add_wrapped(wrap_pyfunction!(python::sql))?;
    Ok(())
}
