use std::sync::Arc;

use crate::error::CatalogResult;

/// Function implementation reference.
pub type FunctionRef = Arc<dyn Function>;

/// A function registered in a catalog.
pub trait Function: Sync + Send {
    /// Create/extract a Python object that subclasses the Function ABC.
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}

/// A wrapper around an opaque Python object that implements the `Function` ABC.
#[cfg(feature = "python")]
pub struct PyFunctionWrapper {
    /// The inner Python object (an instance of `daft.catalog.Function`).
    pub inner: pyo3::Py<pyo3::PyAny>,
}

#[cfg(feature = "python")]
impl Clone for PyFunctionWrapper {
    fn clone(&self) -> Self {
        pyo3::Python::attach(|py| Self {
            inner: self.inner.clone_ref(py),
        })
    }
}

#[cfg(feature = "python")]
impl PyFunctionWrapper {
    pub fn new(inner: pyo3::Py<pyo3::PyAny>) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "python")]
impl Function for PyFunctionWrapper {
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        Ok(self.inner.clone_ref(py))
    }
}

/// Convenience: allow `CatalogResult<Option<FunctionRef>>` to be constructed easily.
#[cfg(feature = "python")]
pub fn function_from_py(inner: pyo3::Py<pyo3::PyAny>) -> CatalogResult<FunctionRef> {
    Ok(Arc::new(PyFunctionWrapper::new(inner)))
}
