use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::{DaftError as CommonDaftError, DaftResult};
#[cfg(feature = "python")]
use daft_core::python::PySeries;
use daft_core::series::Series;
#[cfg(feature = "python")]
use pyo3::PyRef;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny};

use crate::error::CatalogError as DaftError;

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

    /// Put key-value pairs into the KV store (vectorized)
    #[cfg(feature = "python")]
    fn put(&self, key: &Series, value: &Series) -> DaftResult<Series>;

    /// Get a single value by key, returns a Python object
    #[cfg(feature = "python")]
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>>;

    /// Optional schema fields for columns filtering
    fn schema_fields(&self) -> Vec<String> {
        Vec::new()
    }
}

#[cfg(feature = "python")]
/// A wrapper for Python KV Store objects
#[derive(Debug)]
pub struct PyKVStoreWrapper {
    /// The Python KV Store object
    inner: Py<PyAny>,
    /// Cached name for efficiency
    name: String,
    /// Cached backend type for efficiency
    backend_type: String,
}

#[cfg(feature = "python")]
impl PyKVStoreWrapper {
    /// Create a new wrapper from a Python KV Store object
    pub fn new(py_kv_store: Py<PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
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
    pub fn inner(&self) -> &Py<PyAny> {
        &self.inner
    }
}

#[cfg(feature = "python")]
impl Clone for PyKVStoreWrapper {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
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

    fn put(&self, key: &Series, value: &Series) -> DaftResult<Series> {
        Python::attach(|py| {
            // Get the Python KV Store object
            let py_kv_store = self.inner.bind(py);

            // Convert Rust Series to Python objects
            let py_key =
                PySeries::from(key.clone())
                    .to_pylist(py)
                    .map_err(|e| DaftError::DaftError {
                        error: CommonDaftError::ValueError(format!(
                            "Failed to convert key to Python object: {}",
                            e
                        )),
                    })?;

            let py_value =
                PySeries::from(value.clone())
                    .to_pylist(py)
                    .map_err(|e| DaftError::DaftError {
                        error: CommonDaftError::ValueError(format!(
                            "Failed to convert value to Python object: {}",
                            e
                        )),
                    })?;

            // Call the put method on the Python KV Store object
            let result = py_kv_store
                .call_method1("put", (py_key, py_value))
                .map_err(|e| DaftError::DaftError {
                    error: CommonDaftError::ValueError(format!(
                        "Failed to call put method on KV store: {}",
                        e
                    )),
                })?;

            // Convert Python result back to Rust Series
            // For now, we'll return a simple success indicator
            // In the future, this should return the actual result from the KV store
            let py_series: PyRef<PySeries> =
                result.extract().map_err(|e| DaftError::DaftError {
                    error: CommonDaftError::ValueError(format!(
                        "Failed to extract result as PySeries: {}",
                        e
                    )),
                })?;

            Ok(py_series.series.clone())
        })
    }

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let py_kv_store = self.inner.bind(py);
        let py_key = pyo3::types::PyString::new(py, key);
        let result = py_kv_store.call_method1("get", (py_key,))?;
        Ok(result.into())
    }
}

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct MemoryKVStore {
    name: String,
    store: Mutex<HashMap<String, Py<PyAny>>>,
}

#[cfg(feature = "python")]
impl MemoryKVStore {
    pub fn new(name: String) -> Self {
        Self {
            name,
            store: Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(feature = "python")]
impl KVStore for MemoryKVStore {
    fn name(&self) -> &str {
        &self.name
    }
    fn backend_type(&self) -> &'static str {
        "memory"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn put(&self, key: &Series, value: &Series) -> DaftResult<Series> {
        use daft_core::{datatypes::DataType, python::PySeries as DPS};
        use pyo3::Python;
        Python::attach(|py| {
            let k_py = DPS::from(key.clone());
            let v_py = DPS::from(value.clone());
            let k_list = k_py.to_pylist(py).map_err(|e| DaftError::DaftError {
                error: CommonDaftError::ValueError(format!("Failed to convert key: {}", e)),
            })?;
            let v_list = v_py.to_pylist(py).map_err(|e| DaftError::DaftError {
                error: CommonDaftError::ValueError(format!("Failed to convert value: {}", e)),
            })?;
            let n = k_list.len();
            let mut guard = self.store.lock().unwrap();
            for i in 0..n {
                let k_item = k_list.get_item(i).unwrap();
                let key_str: String = {
                    match k_item.extract() {
                        Ok(s) => s,
                        Err(_) => {
                            // Fallback to str() representation
                            let s_obj = k_item.call_method0("__str__").unwrap();
                            s_obj.extract().unwrap_or_default()
                        }
                    }
                };
                let v_item = v_list.get_item(i).unwrap();
                guard.insert(key_str, v_item.into());
            }
            let s = Series::full_null("result", &DataType::Null, n);
            Ok(s)
        })
    }

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        let guard = self.store.lock().unwrap();
        match guard.get(key) {
            Some(obj) => Ok(obj.clone_ref(py)),
            None => Ok(py.None().into()),
        }
    }

    fn schema_fields(&self) -> Vec<String> {
        let n = self.name.to_lowercase();
        if n.contains("embedding") {
            vec!["embedding".to_string()]
        } else if n.contains("meta") {
            vec!["metadata".to_string()]
        } else {
            vec!["value".to_string()]
        }
    }
}

#[cfg(feature = "python")]
impl MemoryKVStore {
    pub fn put_one(&self, _py: Python<'_>, key: &str, value: Bound<PyAny>) -> PyResult<()> {
        let mut guard = self.store.lock().unwrap();
        guard.insert(key.to_string(), value.into());
        Ok(())
    }
}
