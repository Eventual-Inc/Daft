use std::sync::Arc;

use daft_catalog::{python::{PyCatalog, PyCatalogImpl}, Namespace};
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

    pub fn exec(&self, input: &str) -> PyResult<()> {
        todo!()
    }

    pub fn current_catalog(&self) -> PyResult<()> {
        todo!()
    }

    pub fn current_namespace(&self) -> PyResult<Namespace> {
        todo!()
    }

    pub fn attach(&self, catalog: PyObject, alias: String) -> PyResult<()> {
        Ok(self.0.attach(Arc::new(PyCatalogImpl::from(catalog)), alias)?)
    }

    pub fn detach(&self, catalog: &str) -> PyResult<()> {
        Ok(self.0.detach(catalog)?)
    }

    pub fn create_catalog(&self, name: &str) -> PyResult<PyCatalog> {
        todo!()
    }

    pub fn create_namespace(&self, name: &str) -> Namespace {
        todo!()
    }

    pub fn create_table(&self, name: &str, source: Option<PyObject>) -> PyResult<()> {
        todo!()
    }

    pub fn get_catalog(&self, name: &str) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let catalog = self.0.get_catalog(name)?;
            let catalog = catalog.to_py(py);
            Ok(catalog)
        })
    }

    pub fn get_namespace(&self, name: &str) -> Namespace {
        todo!()
    }

    pub fn get_table(&self, name: &str) -> PyResult<()> {
        todo!()
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_catalogs(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_catalogs(pattern)?)
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_namespaces(&self, pattern: Option<&str>) -> PyResult<()> {
        todo!()
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_tables(&self, pattern: Option<&str>) -> PyResult<()> {
        todo!()
    }

    pub fn set_catalog(&self, name: &str) -> PyResult<()> {
        Ok(self.0.set_catalog(name)?)
    }

    pub fn set_namespace(&self, name: String) {
        todo!()
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
