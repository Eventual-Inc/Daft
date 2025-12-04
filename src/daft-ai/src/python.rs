use pyo3::{Bound, Py, PyAny, PyResult, Python, intern, prelude::*, types::PyModule};

use crate::provider::Provider;

/// Implement the Provider trait for a python Provider(ABC)
#[derive(Debug)]
pub struct PyProviderWrapper(Py<PyAny>);

impl From<Py<PyAny>> for PyProviderWrapper {
    fn from(value: Py<PyAny>) -> Self {
        Self(value)
    }
}

impl Provider for PyProviderWrapper {
    fn name(&self) -> String {
        Python::attach(|py| {
            let provider = self.0.bind(py);
            let name = provider
                .getattr(intern!(py, "name"))
                .expect("Provider.name should never fail");
            let name: String = name.extract().expect("name must be a string");
            name
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self.0.clone_ref(py))
    }
}

pub fn register_modules(_: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
