use std::sync::Arc;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A reference to a KV Store implementation
pub type KVStoreRef = Arc<dyn KVStore>;

/// Trait for KV Store implementations
pub trait KVStore: Send + Sync + std::fmt::Debug {
    /// Returns the name of the KV store
    fn name(&self) -> &str;

    /// Returns the backend type (e.g., "lance", "lmdb", "redis", "memory")
    fn backend_type(&self) -> &str;

    /// Returns self as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

#[cfg(feature = "python")]
/// A wrapper for Python KV Store objects
#[derive(Debug)]
pub struct PyKVStoreWrapper {
    /// The Python KV Store object
    inner: PyObject,
    /// Cached name for efficiency
    name: String,
    /// Cached backend type for efficiency
    backend_type: String,
}

#[cfg(feature = "python")]
impl PyKVStoreWrapper {
    /// Create a new wrapper from a Python KV Store object
    pub fn new(py_kv_store: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            let obj = py_kv_store.bind(py);

            // Get the name property
            let name: String = obj.getattr("name")?.extract()?;

            // Get the backend_type property
            let backend_type: String = obj.getattr("backend_type")?.extract()?;

            Ok(Self {
                inner: py_kv_store,
                name,
                backend_type,
            })
        })
    }

    /// Get the underlying Python object
    pub fn inner(&self) -> &PyObject {
        &self.inner
    }
}

#[cfg(feature = "python")]
impl Clone for PyKVStoreWrapper {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Self {
            inner: self.inner.clone_ref(py),
            name: self.name.clone(),
            backend_type: self.backend_type.clone(),
        })
    }
}

#[cfg(feature = "python")]
impl KVStore for PyKVStoreWrapper {
    fn name(&self) -> &str {
        &self.name
    }

    fn backend_type(&self) -> &str {
        &self.backend_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
