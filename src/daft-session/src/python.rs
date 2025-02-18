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

    pub fn attach_catalog(&self, catalog: PyObject, alias: String) -> PyResult<()> {
        Ok(self
            .0
            .attach(Arc::new(PyCatalogImpl::from(catalog)), alias)?)
    }

    pub fn detach_catalog(&self, catalog: &str) -> PyResult<()> {
        Ok(self.0.detach(catalog)?)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
