use std::sync::Arc;

use daft_dsl::functions::ScalarFunctionFactory;

use super::error::CatalogResult;

/// A reference to a [`ScalarFunctionFactory`].
pub type ScalarFunctionFactoryRef = Arc<dyn ScalarFunctionFactory>;

/// Function implementation reference.
pub type FunctionRef = Arc<dyn Function>;

/// A function registered in a catalog.
pub trait Function: Sync + Send + std::fmt::Debug {
    /// Whether this function is a Python UDF.
    ///
    /// When `true`, callers should use [`to_py`](Self::to_py) to obtain the
    /// Python callable. When `false`, callers should use
    /// [`to_scalar_function_factory`](Self::to_scalar_function_factory) to
    /// obtain the native scalar function factory.
    fn is_python(&self) -> bool;

    /// Create/extract a Python object that subclasses the Function ABC.
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;

    /// Extract a [`ScalarFunctionFactory`] from this function.
    fn to_scalar_function_factory(&self) -> ScalarFunctionFactoryRef;
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
impl std::fmt::Debug for PyFunctionWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyFunctionWrapper").finish_non_exhaustive()
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
    fn is_python(&self) -> bool {
        true
    }

    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        Ok(self.inner.clone_ref(py))
    }

    fn to_scalar_function_factory(&self) -> ScalarFunctionFactoryRef {
        unreachable!("Python functions should use to_py instead of to_scalar_function_factory")
    }
}

/// Convert a Python object into a FunctionRef
#[cfg(feature = "python")]
pub fn pyobj_to_function(obj: pyo3::Bound<'_, pyo3::PyAny>) -> CatalogResult<FunctionRef> {
    Ok(Arc::new(PyFunctionWrapper::new(obj.unbind())))
}
