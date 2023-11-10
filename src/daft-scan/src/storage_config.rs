use std::sync::Arc;

use common_io_config::IOConfig;
use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use {
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
    Python(Arc<PythonStorageConfig>),
}

/// Storage configuration for the Rust-native I/O layer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct NativeStorageConfig {
    pub io_config: Option<IOConfig>,
    pub multithreaded_io: bool,
}

impl NativeStorageConfig {
    pub fn new_internal(multithreaded_io: bool, io_config: Option<IOConfig>) -> Self {
        Self {
            io_config,
            multithreaded_io,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl NativeStorageConfig {
    #[new]
    pub fn new(multithreaded_io: bool, io_config: Option<python::IOConfig>) -> Self {
        Self::new_internal(multithreaded_io, io_config.map(|c| c.config))
    }

    #[getter]
    pub fn io_config(&self) -> Option<python::IOConfig> {
        self.io_config.clone().map(|c| c.into())
    }

    #[getter]
    pub fn multithreaded_io(&self) -> bool {
        self.multithreaded_io
    }
}

/// Storage configuration for the legacy Python I/O layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg(feature = "python")]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct PythonStorageConfig {
    /// IOConfig is used when constructing Python filesystems (PyArrow or fsspec filesystems)
    /// and also used for globbing (since we have no Python-based globbing anymore)
    pub io_config: Option<IOConfig>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PythonStorageConfig {
    #[new]
    pub fn new(io_config: Option<python::IOConfig>) -> Self {
        Self {
            io_config: io_config.map(|c| c.config),
        }
    }

    #[getter]
    pub fn io_config(&self) -> Option<python::IOConfig> {
        self.io_config
            .as_ref()
            .map(|c| python::IOConfig { config: c.clone() })
    }
}

#[cfg(feature = "python")]
impl PartialEq for PythonStorageConfig {
    fn eq(&self, other: &Self) -> bool {
        self.io_config.eq(&other.io_config)
    }
}

#[cfg(feature = "python")]
impl Eq for PythonStorageConfig {}

#[cfg(feature = "python")]
impl Hash for PythonStorageConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.io_config.hash(state)
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
        Self(Arc::new(StorageConfig::Python(config.into())))
    }

    /// Get the underlying storage config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use StorageConfig::*;

        match self.0.as_ref() {
            Native(config) => config.as_ref().clone().into_py(py),
            Python(config) => config.as_ref().clone().into_py(py),
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
