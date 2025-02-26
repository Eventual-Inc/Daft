use daft_catalog::{
    python::{PyCatalogWrapper, PyIdentifier, PyTable, PyTableSource, PyTableWrapper},
    Identifier,
};
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

    pub fn attach_table(&self, table: PyObject, alias: String) -> PyResult<()> {
        Ok(self.0.attach_table(PyTableWrapper::wrap(table), alias)?)
    }

    pub fn detach_catalog(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_catalog(alias)?)
    }

    pub fn detach_table(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_table(alias)?)
    }

    pub fn create_temp_table(
        &self,
        name: String,
        source: &PyTableSource,
        replace: bool,
    ) -> PyResult<PyTable> {
        let table = self.0.create_temp_table(name, source.as_ref(), replace)?;
        let table = PyTable::new(table);
        Ok(table)
    }

    pub fn current_catalog(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        self.0.current_catalog()?.map(|c| c.to_py(py)).transpose()
    }

    pub fn current_namespace(&self) -> PyResult<Option<PyIdentifier>> {
        if let Some(namespace) = self.0.current_namespace()? {
            let ident = Identifier::from_path(namespace)?;
            let ident = PyIdentifier::from(ident);
            return Ok(Some(ident));
        }
        Ok(None)
    }

    pub fn get_catalog(&self, py: Python<'_>, name: &str) -> PyResult<PyObject> {
        self.0.get_catalog(name)?.to_py(py)
    }

    pub fn get_table(&self, py: Python<'_>, ident: &PyIdentifier) -> PyResult<PyObject> {
        self.0.get_table(ident.as_ref())?.to_py(py)
    }

    pub fn has_catalog(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_catalog(name))
    }

    pub fn has_table(&self, ident: &PyIdentifier) -> PyResult<bool> {
        Ok(self.0.has_table(ident.as_ref()))
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_catalogs(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_catalogs(pattern)?)
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_tables(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_tables(pattern)?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_catalog(&self, ident: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_catalog(ident)?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_namespace(&self, ident: Option<&PyIdentifier>) -> PyResult<()> {
        Ok(self.0.set_namespace(ident.map(|i| i.as_ref()))?)
    }
}

impl From<&PySession> for Session {
    fn from(sess: &PySession) -> Self {
        sess.0.clone()
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
