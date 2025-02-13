use daft_catalog::{python::PyCatalog, Namespace};
use pyo3::prelude::*;

use crate::Session;

/// Bridge from session.py to session.rs
#[pyclass]
pub struct PySession(Session);

#[pymethods]
impl PySession {

    #[staticmethod]
    pub fn empty() -> Self {
        todo!()
    }

    pub fn exec(&self, input: &str) -> PyResult<()> {
        todo!()
    }

    pub fn current_catalog(&self) -> PyResult<PyCatalog> {
        todo!()
    }

    pub fn current_namespace(&self) -> PyResult<Namespace> {
        todo!()
    }


    pub fn attach(&self, name: String, catalog: PyObject) -> PyResult<()> {
        todo!()
    }

    pub fn detach(&self, name: &str) {
        todo!()
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

    pub fn get_catalog(&self, name: &str) -> PyResult<PyCatalog> {
        todo!()
    }

    pub fn get_namespace(&self, name: &str) -> Namespace {
        todo!()
    }

    pub fn get_table(&self, name: &str) -> PyResult<()> {
        todo!()
    }

    pub fn list_catalogs(&self, pattern: Option<&str>) -> PyResult<()> {
        todo!()
    }

    pub fn list_namespaces(&self, pattern: Option<&str>) -> PyResult<()> {
        todo!()
    }

    pub fn list_tables(&self, pattern: Option<&str>) -> PyResult<()> {
        todo!()
    }

    pub fn set_catalog(&self, name: &str) {
        todo!()
    }

    pub fn set_namespace(&self, name: &str) {
        todo!()
    }

}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
