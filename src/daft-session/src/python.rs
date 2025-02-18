use daft_catalog::python::{PyCatalogWrapper, PyIdentifier, PyTableWrapper};
use pyo3::prelude::*;

use crate::Session;

#[pyclass]
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
            .attach_catalog(PyCatalogWrapper::wrap(catalog), alias)?)
    }

    pub fn current_catalog(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.0.current_catalog()?.to_py(py)
    }

    pub fn detach_catalog(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_catalog(alias)?)
    }

    pub fn detach_table(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_table(alias)?)
    }

    pub fn attach_table(&self, table: PyObject, alias: String) -> PyResult<()> {
        Ok(self.0.attach_table(PyTableWrapper::wrap(table), alias)?)
    }

    pub fn get_catalog(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        self.0.get_catalog(name)?.to_py(py)
    }

    pub fn get_table(&self, py: Python<'_>, name: &PyIdentifier) -> PyResult<PyObject> {
        self.0.get_table(name.as_ref())?.to_py(py)
    }

    pub fn has_catalog(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_catalog(name))
    }

    pub fn has_table(&self, name: &PyIdentifier) -> PyResult<bool> {
        Ok(self.0.has_table(name.as_ref()))
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_catalogs(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_catalogs(pattern)?)
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_tables(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_tables(pattern)?)
    }

    pub fn set_catalog(&self, name: &str) -> PyResult<()> {
        Ok(self.0.set_catalog(name)?)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
