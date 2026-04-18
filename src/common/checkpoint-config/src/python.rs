//! PyO3 wrappers for the config types.

use pyo3::prelude::*;

use crate::CheckpointStoreConfig;

#[pyclass(
    module = "daft.daft",
    name = "CheckpointStoreConfig",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyCheckpointStoreConfig {
    pub config: CheckpointStoreConfig,
}

#[pymethods]
impl PyCheckpointStoreConfig {
    #[staticmethod]
    pub fn object_store(prefix: String, io_config: common_io_config::python::IOConfig) -> Self {
        Self {
            config: CheckpointStoreConfig::ObjectStore {
                prefix,
                io_config: Box::new(io_config.config),
            },
        }
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.config)
    }
}

impl From<CheckpointStoreConfig> for PyCheckpointStoreConfig {
    fn from(config: CheckpointStoreConfig) -> Self {
        Self { config }
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyCheckpointStoreConfig>()?;
    Ok(())
}
