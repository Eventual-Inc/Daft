#[cfg(feature = "python")]
use common_io_config::python::IOConfig as PyIOConfig;
#[cfg(feature = "python")]
use daft_dsl::functions::kv::lance::LanceKVStore;
#[cfg(feature = "python")]
use daft_dsl::functions::kv::{KVConfig, LanceConfig};
use daft_io::IOConfig;
#[cfg(feature = "python")]
use pyo3::{PyResult, Python, prelude::*};

#[cfg(feature = "python")]
#[pyclass(name = "_LanceKVStore")]
#[derive(Debug)]
pub struct PyLanceKVStore {
    pub inner: LanceKVStore,
    io_config: Option<IOConfig>,
}

#[cfg(not(feature = "python"))]
#[derive(Debug)]
pub struct PyLanceKVStore;

#[cfg(feature = "python")]
#[pymethods]
impl PyLanceKVStore {
    #[new]
    pub fn new(
        name: String,
        uri: String,
        key_column: String,
        batch_size: usize,
        io_config: Option<PyIOConfig>,
    ) -> Self {
        let io_config_native = io_config.map(|c| c.config);
        let store = LanceKVStore::new(name, uri, key_column, batch_size, io_config_native.clone());
        Self {
            inner: store,
            io_config: io_config_native,
        }
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    pub fn get(&self, py: Python, key: &str) -> PyResult<pyo3::Py<pyo3::types::PyAny>> {
        self.inner.get(py, key)
    }

    pub fn to_config(&self) -> PyResult<String> {
        let config = LanceConfig::new(self.inner.uri.clone())
            .with_key_column(self.inner.key_column.clone())
            .with_batch_size(self.inner.batch_size);

        let config = if let Some(io_config) = &self.io_config {
            config.with_io_config(io_config.clone())
        } else {
            config
        };

        let kv_config = KVConfig::new().with_lance(config);

        serde_json::to_string(&kv_config).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to serialize config: {}", e))
        })
    }
}
