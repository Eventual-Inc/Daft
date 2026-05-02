//! PyO3 wrappers for the config types.

use common_hashable_float_wrapper::FloatWrapper;
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::{
    CheckpointConfig, CheckpointKeyMode, CheckpointSettings, CheckpointStoreConfig,
    KeyFilteringSettings,
};

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

/// Tuning knobs for the key-filtering anti-join used to skip already-sealed
/// source rows. All fields default to `None`, meaning "use the engine
/// default."
#[pyclass(
    module = "daft.daft",
    name = "KeyFilteringSettings",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyKeyFilteringSettings {
    pub settings: KeyFilteringSettings,
}

#[pymethods]
impl PyKeyFilteringSettings {
    #[new]
    #[pyo3(signature = (
        num_workers = None,
        cpus_per_worker = None,
        keys_load_batch_size = None,
        max_concurrency_per_worker = None,
        filter_batch_size = None,
    ))]
    pub fn new(
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> PyResult<Self> {
        let settings = KeyFilteringSettings {
            num_workers,
            cpus_per_worker: cpus_per_worker.map(FloatWrapper),
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        };
        settings.validate().map_err(PyValueError::new_err)?;
        Ok(Self { settings })
    }

    #[getter]
    pub fn num_workers(&self) -> Option<usize> {
        self.settings.num_workers
    }

    #[getter]
    pub fn cpus_per_worker(&self) -> Option<f64> {
        self.settings.cpus_per_worker.as_ref().map(|w| w.0)
    }

    #[getter]
    pub fn keys_load_batch_size(&self) -> Option<usize> {
        self.settings.keys_load_batch_size
    }

    #[getter]
    pub fn max_concurrency_per_worker(&self) -> Option<usize> {
        self.settings.max_concurrency_per_worker
    }

    #[getter]
    pub fn filter_batch_size(&self) -> Option<usize> {
        self.settings.filter_batch_size
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.settings)
    }
}

/// Per-source checkpoint configuration: bundles the store, the key column,
/// and strategy-specific tuning into a single object.
#[pyclass(
    module = "daft.daft",
    name = "CheckpointConfig",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyCheckpointConfig {
    pub config: CheckpointConfig,
}

#[pymethods]
impl PyCheckpointConfig {
    #[new]
    #[pyo3(signature = (store, on = None, settings = None))]
    pub fn new(
        store: PyCheckpointStoreConfig,
        on: Option<String>,
        settings: Option<PyKeyFilteringSettings>,
    ) -> Self {
        let key_mode = match on {
            Some(col) => CheckpointKeyMode::RowLevel { key_column: col },
            None => CheckpointKeyMode::FilePath,
        };
        let settings = settings
            .map(|s| CheckpointSettings::KeyFiltering(s.settings))
            .unwrap_or_default();
        Self {
            config: CheckpointConfig {
                store: store.config,
                key_mode,
                settings,
            },
        }
    }

    #[getter]
    pub fn key_column(&self) -> Option<String> {
        self.config.key_column().map(|s| s.to_string())
    }

    #[getter]
    pub fn is_file_path_mode(&self) -> bool {
        self.config.is_file_path_mode()
    }

    #[getter]
    pub fn settings(&self) -> PyKeyFilteringSettings {
        let CheckpointSettings::KeyFiltering(kf) = &self.config.settings;
        PyKeyFilteringSettings {
            settings: kf.clone(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self.config)
    }
}

impl From<CheckpointConfig> for PyCheckpointConfig {
    fn from(config: CheckpointConfig) -> Self {
        Self { config }
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyCheckpointStoreConfig>()?;
    parent.add_class::<PyKeyFilteringSettings>()?;
    parent.add_class::<PyCheckpointConfig>()?;
    Ok(())
}
