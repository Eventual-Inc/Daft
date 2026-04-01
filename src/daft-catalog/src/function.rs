use std::sync::Arc;

use crate::error::CatalogResult;

/// Function implementation reference.
pub type FunctionRef = Arc<dyn Function>;

/// A function registered in a catalog, backed by a Python callable.
#[cfg(debug_assertions)]
pub trait Function: Sync + Send + std::fmt::Debug {
    /// Returns the module name where the function is defined.
    fn module_name(&self) -> String;

    /// Returns the binding name of the function within its module.
    fn binding_name(&self) -> String;

    /// Create/extract a Python object that subclasses the Function ABC.
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}

#[cfg(not(debug_assertions))]
pub trait Function: Sync + Send {
    /// Returns the module name where the function is defined.
    fn module_name(&self) -> String;

    /// Returns the binding name of the function within its module.
    fn binding_name(&self) -> String;

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

#[cfg(all(feature = "python", debug_assertions))]
impl std::fmt::Debug for PyFunctionWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyFunctionWrapper").finish()
    }
}

#[cfg(feature = "python")]
impl Function for PyFunctionWrapper {
    fn module_name(&self) -> String {
        use pyo3::{Python, types::PyAnyMethods};
        Python::attach(|py| {
            self.inner
                .bind(py)
                .getattr("module_name")
                .and_then(|attr| attr.extract::<String>())
                .unwrap_or_default()
        })
    }

    fn binding_name(&self) -> String {
        use pyo3::{Python, types::PyAnyMethods};
        Python::attach(|py| {
            self.inner
                .bind(py)
                .getattr("binding_name")
                .and_then(|attr| attr.extract::<String>())
                .unwrap_or_default()
        })
    }

    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        Ok(self.inner.clone_ref(py))
    }
}

/// Convenience: allow `CatalogResult<Option<FunctionRef>>` to be constructed easily.
#[cfg(feature = "python")]
pub fn function_from_py(inner: pyo3::Py<pyo3::PyAny>) -> CatalogResult<FunctionRef> {
    Ok(Arc::new(PyFunctionWrapper::new(inner)))
}
