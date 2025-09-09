use pyo3::{Bound, PyObject, PyResult, Python, intern, prelude::*, types::PyModule};

use crate::provider::Provider;

/// Implement the Provider trait for a python Provider(ABC)
#[derive(Debug)]
pub struct PyProviderWrapper(PyObject);

impl From<PyObject> for PyProviderWrapper {
    fn from(value: PyObject) -> Self {
        Self(value)
    }
}

impl Provider for PyProviderWrapper {
    fn name(&self) -> String {
        Python::with_gil(|py| {
            let provider = self.0.bind(py);
            let name = provider
                .getattr(intern!(py, "name"))
                .expect("Provider.name should never fail");
            let name: String = name.extract().expect("name must be a string");
            name
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.0.clone_ref(py))
    }
}

pub fn register_modules(_: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
