use pyo3::prelude::*;

use crate::Session;

#[pyclass]
#[allow(dead_code)]
pub struct PySession(Session);

#[pymethods]
impl PySession {
    #[staticmethod]
    pub fn empty() -> Self {
        Self(Session::empty())
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
