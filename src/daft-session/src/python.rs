use std::sync::Arc;

use daft_ai::{provider::ProviderRef, python::PyProviderWrapper};
use daft_catalog::{
    Identifier,
    python::{PyIdentifier, PyTableSource, pyobj_to_catalog, pyobj_to_table},
};
use daft_dsl::functions::python::WrappedUDFClass;
use pyo3::prelude::*;

use crate::Session;

#[pyclass]
pub struct PySession(Session);

impl PySession {
    pub fn session(&self) -> &Session {
        &self.0
    }
}

#[pymethods]
impl PySession {
    #[staticmethod]
    pub fn empty() -> Self {
        Self(Session::empty())
    }

    pub fn attach_catalog(&self, catalog: Bound<PyAny>, alias: String) -> PyResult<()> {
        Ok(self.0.attach_catalog(pyobj_to_catalog(catalog)?, alias)?)
    }

    pub fn attach_provider(&self, provider: Bound<PyAny>, alias: String) -> PyResult<()> {
        Ok(self
            .0
            .attach_provider(pyobj_to_provider(provider)?, alias)?)
    }

    pub fn attach_table(&self, table: Bound<PyAny>, alias: String) -> PyResult<()> {
        Ok(self.0.attach_table(pyobj_to_table(table)?, alias)?)
    }

    pub fn detach_catalog(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_catalog(alias)?)
    }

    pub fn detach_provider(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_provider(alias)?)
    }

    pub fn detach_table(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_table(alias)?)
    }

    pub fn create_temp_table(
        &self,
        name: String,
        source: &PyTableSource,
        replace: bool,
        py: Python,
    ) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0
            .create_temp_table(name, source.as_ref(), replace)?
            .to_py(py)
    }

    pub fn current_catalog(&self, py: Python<'_>) -> PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
        self.0.current_catalog()?.map(|c| c.to_py(py)).transpose()
    }

    pub fn current_namespace(&self) -> PyResult<Option<PyIdentifier>> {
        if let Some(namespace) = self.0.current_namespace()? {
            let ident = Identifier::try_new(namespace)?;
            let ident = PyIdentifier::from(ident);
            return Ok(Some(ident));
        }
        Ok(None)
    }

    pub fn current_provider(&self, py: Python<'_>) -> PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
        self.0.current_provider()?.map(|p| p.to_py(py)).transpose()
    }

    pub fn current_model(&self) -> PyResult<Option<String>> {
        Ok(self.0.current_model()?)
    }

    pub fn get_catalog(&self, py: Python<'_>, name: &str) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0.get_catalog(name)?.to_py(py)
    }

    pub fn get_provider(&self, py: Python<'_>, name: &str) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0.get_provider(name)?.to_py(py)
    }

    pub fn get_table(
        &self,
        py: Python<'_>,
        ident: &PyIdentifier,
    ) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0.get_table(ident.as_ref())?.to_py(py)
    }

    pub fn has_catalog(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_catalog(name))
    }

    pub fn has_provider(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_provider(name))
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

    #[pyo3(signature = (ident))]
    pub fn set_provider(&self, ident: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_provider(ident)?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_model(&self, ident: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_model(ident)?)
    }

    #[pyo3(signature = (function, alias = None))]
    pub fn attach_function(
        &self,
        function: pyo3::Py<pyo3::PyAny>,
        alias: Option<String>,
    ) -> PyResult<()> {
        let wrapped = WrappedUDFClass {
            inner: Arc::new(function),
        };

        self.0.attach_function(wrapped, alias)?;

        Ok(())
    }

    pub fn detach_function(&self, alias: &str) -> PyResult<()> {
        self.0.detach_function(alias)?;
        Ok(())
    }
}

fn pyobj_to_provider(obj: Bound<PyAny>) -> PyResult<ProviderRef> {
    // no current rust-based providers, so just wrap
    Ok(Arc::new(PyProviderWrapper::from(obj.unbind())))
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    Ok(())
}
