use std::sync::Arc;

use common_error::DaftResult;
use common_io_config::IOConfig;
use common_py_serde::impl_bincode_py_state_serialization;
use common_runtime::{get_io_runtime, RuntimeRef};
use daft_io::{get_io_client, IOClient};
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    common_io_config::python,
    pyo3::{pyclass, pymethods, IntoPy, PyObject, PyResult, Python},
    std::hash::Hash,
};

/// Configuration for interacting with a particular storage backend, using a particular
/// I/O layer implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum StorageConfig {
    Native(Arc<NativeStorageConfig>),
}

impl StorageConfig {
    pub fn get_io_client_and_runtime(&self) -> DaftResult<(RuntimeRef, Arc<IOClient>)> {
        // Grab an IOClient and Runtime
        // TODO: This should be cleaned up and hidden behind a better API from daft-io
        match self {
            Self::Native(cfg) => {
                let multithreaded_io = cfg.multithreaded_io;
                Ok((
                    get_io_runtime(multithreaded_io),
                    get_io_client(
                        multithreaded_io,
                        Arc::new(cfg.io_config.clone().unwrap_or_default()),
                    )?,
                ))
            }
        }
    }

    #[must_use]
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Native(_) => "Native",
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Native(source) => source.multiline_display(),
        }
    }
}

/// Storage configuration for the Rust-native I/O layer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct NativeStorageConfig {
    pub io_config: Option<IOConfig>,
    pub multithreaded_io: bool,
}

impl NativeStorageConfig {
    #[must_use]
    pub fn new_internal(multithreaded_io: bool, io_config: Option<IOConfig>) -> Self {
        Self {
            io_config,
            multithreaded_io,
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(io_config) = &self.io_config {
            res.push(format!(
                "IO config = {}",
                io_config.multiline_display().join(", ")
            ));
        }
        res.push(format!("Use multithreading = {}", self.multithreaded_io));
        res
    }
}

impl Default for NativeStorageConfig {
    fn default() -> Self {
        Self::new_internal(true, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl NativeStorageConfig {
    #[new]
    #[must_use]
    pub fn new(multithreaded_io: bool, io_config: Option<python::IOConfig>) -> Self {
        Self::new_internal(multithreaded_io, io_config.map(|c| c.config))
    }

    #[getter]
    #[must_use]
    pub fn io_config(&self) -> Option<python::IOConfig> {
        self.io_config.clone().map(std::convert::Into::into)
    }

    #[getter]
    #[must_use]
    pub fn multithreaded_io(&self) -> bool {
        self.multithreaded_io
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

    /// Get the underlying storage config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use StorageConfig::Native;

        match self.0.as_ref() {
            Native(config) => config.as_ref().clone().into_py(py),
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
