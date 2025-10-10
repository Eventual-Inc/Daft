use std::sync::Arc;

use common_error::DaftResult;
use common_io_config::IOConfig;
use common_py_serde::impl_bincode_py_state_serialization;
use common_runtime::{RuntimeRef, get_io_runtime};
use daft_io::{IOClient, get_io_client};
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    common_io_config::python,
    pyo3::{PyResult, Python, pyclass, pymethods},
    std::hash::Hash,
};

/// Configuration for interacting with a particular storage backend, using a particular
/// I/O layer implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct StorageConfig {
    // TODO: store Arc<IOConfig> instead
    pub io_config: Option<IOConfig>,
    pub multithreaded_io: bool,
}

impl StorageConfig {
    #[must_use]
    pub fn new_internal(multithreaded_io: bool, io_config: Option<IOConfig>) -> Self {
        Self {
            io_config,
            multithreaded_io,
        }
    }

    pub fn get_io_client_and_runtime(&self) -> DaftResult<(RuntimeRef, Arc<IOClient>)> {
        // Grab an IOClient and Runtime
        // TODO: This should be cleaned up and hidden behind a better API from daft-io
        let multithreaded_io = self.multithreaded_io;
        Ok((
            get_io_runtime(multithreaded_io),
            get_io_client(
                multithreaded_io,
                Arc::new(self.io_config.clone().unwrap_or_default()),
            )?,
        ))
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new_internal(true, None)
    }
}

impl From<IOConfig> for StorageConfig {
    fn from(io_config: IOConfig) -> Self {
        Self::new_internal(true, Some(io_config))
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl StorageConfig {
    #[new]
    #[must_use]
    #[pyo3(signature = (multithreaded_io, io_config=None))]
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

impl_bincode_py_state_serialization!(StorageConfig);
