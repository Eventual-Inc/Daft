use std::sync::Arc;

use pyo3::{Bound, Py, PyAny, PyResult, Python, intern, prelude::*, types::PyModule};

use crate::provider::Provider;

/// Implement the Provider trait for a python Provider(ABC)
#[derive(Debug)]
pub struct PyProviderWrapper(Py<PyAny>);

impl From<Bound<'_, PyAny>> for PyProviderWrapper {
    fn from(obj: Bound<'_, PyAny>) -> Self {
        Self(obj.unbind())
    }
}

impl PyProviderWrapper {
    pub fn arced(self) -> Arc<Self> {
        Arc::new(self)
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
