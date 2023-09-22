use std::sync::Arc;

use common_io_config::IOConfig;
use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use {
    super::py_object_serde::{deserialize_py_object_optional, serialize_py_object_optional},
    common_io_config::python,
    pyo3::{
        pyclass, pymethods, types::PyBytes, IntoPy, PyObject, PyResult, PyTypeInfo, Python,
        ToPyObject,
    },
    std::hash::{Hash, Hasher},
};

/// Configuration for interacting with a particular storage backend, using a particular
/// I/O layer implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum StorageConfig {
    Native(Arc<NativeStorageConfig>),
    #[cfg(feature = "python")]
    Python(PythonStorageConfig),
}

/// Storage configuration for the Rust-native I/O layer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct NativeStorageConfig {
    pub io_config: Option<IOConfig>,
}

impl NativeStorageConfig {
    pub fn new_internal(io_config: Option<IOConfig>) -> Self {
        Self { io_config }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl NativeStorageConfig {
    #[new]
    pub fn new(io_config: Option<python::IOConfig>) -> Self {
        Self::new_internal(io_config.map(|c| c.config))
    }

    #[getter]
    pub fn io_config(&self) -> Option<python::IOConfig> {
        self.io_config.clone().map(|c| c.into())
    }
}

/// Storage configuration for the legacy Python I/O layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg(feature = "python")]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct PythonStorageConfig {
    /// An fsspec filesystem instance.
    #[serde(
        serialize_with = "serialize_py_object_optional",
        deserialize_with = "deserialize_py_object_optional",
        default
    )]
    pub fs: Option<PyObject>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PythonStorageConfig {
    #[new]
    pub fn new(fs: Option<PyObject>) -> Self {
        Self { fs }
    }
}

#[cfg(feature = "python")]
impl PartialEq for PythonStorageConfig {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| match (&self.fs, &other.fs) {
            (Some(self_fs), Some(other_fs)) => self_fs.as_ref(py).eq(other_fs.as_ref(py)).unwrap(),
            (None, None) => true,
            _ => false,
        })
    }
}

#[cfg(feature = "python")]
impl Eq for PythonStorageConfig {}

#[cfg(feature = "python")]
impl Hash for PythonStorageConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_obj_hash = self
            .fs
            .as_ref()
            .map(|fs| Python::with_gil(|py| fs.as_ref(py).hash()))
            .transpose();
        match py_obj_hash {
            // If Python object is None OR is hashable, hash the Option of the Python-side hash.
            Ok(py_obj_hash) => py_obj_hash.hash(state),
            // Fall back to hashing the pickled Python object.
            Err(_) => Some(serde_json::to_vec(self).unwrap()).hash(state),
        }
    }
}

/// A Python-exposed interface for storage configs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "StorageConfig")
)]
pub struct PyStorageConfig(Arc<StorageConfig>);

#[cfg(feature = "python")]
#[pymethods]
impl PyStorageConfig {
    /// Create from a native storage config.
    #[staticmethod]
    fn native(config: NativeStorageConfig) -> Self {
        Self(Arc::new(StorageConfig::Native(config.into())))
    }

    /// Create from a Python storage config.
    #[staticmethod]
    fn python(config: PythonStorageConfig) -> Self {
        Self(Arc::new(StorageConfig::Python(config)))
    }

    /// Get the underlying storage config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use StorageConfig::*;

        match self.0.as_ref() {
            Native(config) => config.as_ref().clone().into_py(py),
            Python(config) => config.clone().into_py(py),
        }
    }
}

impl_bincode_py_state_serialization!(PyStorageConfig);

impl From<PyStorageConfig> for Arc<StorageConfig> {
    fn from(value: PyStorageConfig) -> Self {
        value.0
    }
}

impl From<Arc<StorageConfig>> for PyStorageConfig {
    fn from(value: Arc<StorageConfig>) -> Self {
        Self(value)
    }
}
