#[cfg(feature = "python")]
use std::sync::Arc;

/// A Python DataSource instance (daft.io.source:DataSource).
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct DataSource {
    #[cfg(feature = "python")]
    pub inner: Arc<pyo3::Py<pyo3::PyAny>>,
}

#[cfg(feature = "python")]
impl DataSource {
    /// Creates a data source from a Python DataSource instance.
    pub fn new(inner: pyo3::Py<pyo3::PyAny>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Returns the source name by reading the instance's `name` property.
    pub fn name(&self) -> pyo3::PyResult<String> {
        pyo3::Python::attach(|py| self.inner.getattr(py, "name")?.extract(py))
    }

    /// Returns the underlying Python DataSource instance.
    pub fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::Py<pyo3::PyAny> {
        self.inner.clone_ref(py)
    }
}
