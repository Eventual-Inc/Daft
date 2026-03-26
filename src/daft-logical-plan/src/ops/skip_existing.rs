use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_hashable_float_wrapper::FloatWrapper;
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use educe::Educe;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Educe, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[educe(PartialEq, Eq, Hash)]
pub struct SkipExistingSpec {
    pub existing_path: Vec<String>,
    pub file_format: FileFormat,
    pub key_column: Vec<String>,
    pub io_config: Option<IOConfig>,
    pub num_workers: Option<usize>,
    pub cpus_per_worker: Option<FloatWrapper<f64>>,
    pub keys_load_batch_size: Option<usize>,
    pub max_concurrency_per_worker: Option<usize>,
    pub filter_batch_size: Option<usize>,
    #[cfg(feature = "python")]
    pub read_kwargs: PyObjectWrapper,
}

impl SkipExistingSpec {
    fn validate_inputs(
        existing_path: &[String],
        key_column: &[String],
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> DaftResult<()> {
        if existing_path.is_empty() || existing_path.iter().any(|p| p.is_empty()) {
            return Err(DaftError::ValueError(
                "[skip_existing] existing_path must be a non-empty list of non-empty paths"
                    .to_string(),
            ));
        }
        if key_column.is_empty() || key_column.iter().any(|c| c.is_empty()) {
            return Err(DaftError::ValueError(
                "[skip_existing] key_column must be a non-empty list of non-empty column names"
                    .to_string(),
            ));
        }
        if matches!(num_workers, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] num_workers must be > 0".to_string(),
            ));
        }
        if matches!(cpus_per_worker, Some(v) if v <= 0.0) {
            return Err(DaftError::ValueError(
                "[skip_existing] cpus_per_worker must be > 0".to_string(),
            ));
        }
        if matches!(keys_load_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] keys_load_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(max_concurrency_per_worker, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] max_concurrency_per_worker must be > 0".to_string(),
            ));
        }
        if matches!(filter_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] filter_batch_size must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        existing_path: Vec<String>,
        file_format: FileFormat,
        key_column: Vec<String>,
        io_config: Option<IOConfig>,
        read_kwargs: PyObjectWrapper,
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> DaftResult<Self> {
        Self::validate_inputs(
            &existing_path,
            &key_column,
            num_workers,
            cpus_per_worker,
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        )?;
        Ok(Self {
            existing_path,
            file_format,
            key_column,
            io_config,
            read_kwargs,
            num_workers,
            cpus_per_worker: cpus_per_worker.map(FloatWrapper),
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        })
    }

    #[cfg(not(feature = "python"))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        existing_path: Vec<String>,
        file_format: FileFormat,
        key_column: Vec<String>,
        io_config: Option<IOConfig>,
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> DaftResult<Self> {
        Self::validate_inputs(
            &existing_path,
            &key_column,
            num_workers,
            cpus_per_worker,
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        )?;
        Ok(Self {
            existing_path,
            file_format,
            key_column,
            io_config,
            num_workers,
            cpus_per_worker: cpus_per_worker.map(FloatWrapper),
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        })
    }
}

#[derive(Educe, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[educe(PartialEq, Eq, Hash)]
pub struct KeyFilteringConfig {
    pub left_key_columns: Vec<String>,
    pub right_key_columns: Vec<String>,
    pub num_workers: Option<usize>,
    pub cpus_per_worker: Option<FloatWrapper<f64>>,
    pub keys_load_batch_size: Option<usize>,
    pub max_concurrency_per_worker: Option<usize>,
    pub filter_batch_size: Option<usize>,
}

impl KeyFilteringConfig {
    fn validate_runtime_inputs(
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> DaftResult<()> {
        if matches!(num_workers, Some(0)) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] num_workers must be > 0".to_string(),
            ));
        }
        if matches!(cpus_per_worker, Some(v) if v <= 0.0) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] cpus_per_worker must be > 0".to_string(),
            ));
        }
        if matches!(keys_load_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] keys_load_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(max_concurrency_per_worker, Some(0)) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] max_concurrency_per_worker must be > 0".to_string(),
            ));
        }
        if matches!(filter_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] filter_batch_size must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_key_columns(
        left_key_columns: &[String],
        right_key_columns: &[String],
    ) -> DaftResult<()> {
        if left_key_columns.is_empty() || left_key_columns.iter().any(|c| c.is_empty()) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] left join keys must be a non-empty list of non-empty column names"
                    .to_string(),
            ));
        }
        if right_key_columns.is_empty() || right_key_columns.iter().any(|c| c.is_empty()) {
            return Err(DaftError::ValueError(
                "[key_filtering_join] right join keys must be a non-empty list of non-empty column names"
                    .to_string(),
            ));
        }
        if left_key_columns.len() != right_key_columns.len() {
            return Err(DaftError::ValueError(
                "[key_filtering_join] left and right join keys must have the same length"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub fn with_key_columns(
        mut self,
        left_key_columns: Vec<String>,
        right_key_columns: Vec<String>,
    ) -> DaftResult<Self> {
        Self::validate_key_columns(&left_key_columns, &right_key_columns)?;
        self.left_key_columns = left_key_columns;
        self.right_key_columns = right_key_columns;
        Ok(self)
    }

    pub fn new(
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> DaftResult<Self> {
        Self::validate_runtime_inputs(
            num_workers,
            cpus_per_worker,
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        )?;
        Ok(Self {
            left_key_columns: vec![],
            right_key_columns: vec![],
            num_workers,
            cpus_per_worker: cpus_per_worker.map(FloatWrapper),
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        sync::Arc,
    };

    use common_file_formats::FileFormat;
    #[cfg(feature = "python")]
    use pyo3::types::PyDictMethods;

    use super::SkipExistingSpec;

    #[cfg(not(feature = "python"))]
    #[test]
    fn test_skip_existing_spec_eq_hash() {
        let spec_a = SkipExistingSpec::new(
            vec!["root".to_string()],
            FileFormat::Csv,
            vec!["id".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let spec_b = SkipExistingSpec::new(
            vec!["root2".to_string()],
            FileFormat::Csv,
            vec!["id".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_ne!(spec_a, spec_b);

        let mut h1 = DefaultHasher::new();
        spec_a.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        spec_b.hash(&mut h2);
        assert_ne!(h1.finish(), h2.finish());
    }

    #[cfg(feature = "python")]
    #[test]
    fn test_skip_existing_spec_eq_hash_kwargs() {
        pyo3::Python::attach(|py| {
            let kwargs_a = {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("delimiter", "|").unwrap();
                common_py_serde::PyObjectWrapper(Arc::new(d.unbind().into()))
            };
            let kwargs_b = {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("delimiter", "|").unwrap();
                common_py_serde::PyObjectWrapper(Arc::new(d.unbind().into()))
            };
            let kwargs_c = {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("delimiter", ",").unwrap();
                common_py_serde::PyObjectWrapper(Arc::new(d.unbind().into()))
            };

            let spec_a = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["id".to_string()],
                None,
                kwargs_a,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

            let spec_b = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["id".to_string()],
                None,
                kwargs_b,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

            let spec_c = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["id".to_string()],
                None,
                kwargs_c,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

            assert_eq!(spec_a, spec_b);

            let mut h1 = DefaultHasher::new();
            spec_a.hash(&mut h1);
            let mut h2 = DefaultHasher::new();
            spec_b.hash(&mut h2);
            assert_eq!(h1.finish(), h2.finish());

            assert_ne!(spec_a, spec_c);

            let mut h1 = DefaultHasher::new();
            spec_a.hash(&mut h1);
            let mut h2 = DefaultHasher::new();
            spec_c.hash(&mut h2);
            assert_ne!(h1.finish(), h2.finish());
        });
    }
}

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "SkipExistingSpec", from_py_object)]
#[derive(Clone)]
pub struct PySkipExistingSpec {
    pub spec: SkipExistingSpec,
}

#[cfg(feature = "python")]
#[pymethods]
impl PySkipExistingSpec {
    #[new]
    #[pyo3(signature = (existing_path, file_format, key_column, io_config=None, read_kwargs=None, num_workers=None, cpus_per_worker=None, keys_load_batch_size=None, max_concurrency_per_worker=None, filter_batch_size=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python,
        existing_path: pyo3::Py<pyo3::PyAny>,
        file_format: FileFormat,
        key_column: pyo3::Py<pyo3::PyAny>,
        io_config: Option<common_io_config::python::IOConfig>,
        read_kwargs: Option<pyo3::Py<pyo3::PyAny>>,
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> PyResult<Self> {
        let existing_path: Vec<String> = if let Ok(s) = existing_path.extract::<String>(py) {
            vec![s]
        } else {
            existing_path.extract::<Vec<String>>(py)?
        };
        let key_column: Vec<String> = if let Ok(s) = key_column.extract::<String>(py) {
            vec![s]
        } else {
            key_column.extract::<Vec<String>>(py)?
        };
        let read_kwargs = PyObjectWrapper(std::sync::Arc::new(
            read_kwargs.unwrap_or_else(|| py.None()),
        ));
        let spec = SkipExistingSpec::new(
            existing_path,
            file_format,
            key_column,
            io_config.map(|cfg| cfg.config),
            read_kwargs,
            num_workers,
            cpus_per_worker,
            keys_load_batch_size,
            max_concurrency_per_worker,
            filter_batch_size,
        )?;
        Ok(Self { spec })
    }

    #[getter]
    pub fn existing_path(&self) -> Vec<String> {
        self.spec.existing_path.clone()
    }

    #[getter]
    pub fn file_format(&self) -> FileFormat {
        self.spec.file_format
    }

    #[getter]
    pub fn key_column(&self) -> Vec<String> {
        self.spec.key_column.clone()
    }

    #[getter]
    pub fn io_config(&self) -> Option<common_io_config::python::IOConfig> {
        self.spec
            .io_config
            .as_ref()
            .map(|c| common_io_config::python::IOConfig { config: c.clone() })
    }

    #[getter]
    pub fn num_workers(&self) -> Option<usize> {
        self.spec.num_workers
    }

    #[getter]
    pub fn cpus_per_worker(&self) -> Option<f64> {
        self.spec.cpus_per_worker.as_ref().map(|v| v.0)
    }

    #[getter]
    pub fn keys_load_batch_size(&self) -> Option<usize> {
        self.spec.keys_load_batch_size
    }

    #[getter]
    pub fn max_concurrency_per_worker(&self) -> Option<usize> {
        self.spec.max_concurrency_per_worker
    }

    #[getter]
    pub fn filter_batch_size(&self) -> Option<usize> {
        self.spec.filter_batch_size
    }

    #[getter]
    pub fn read_kwargs(&self, py: Python) -> Py<PyAny> {
        self.spec.read_kwargs.0.as_ref().clone_ref(py)
    }
}

#[cfg(feature = "python")]
impl From<SkipExistingSpec> for PySkipExistingSpec {
    fn from(spec: SkipExistingSpec) -> Self {
        Self { spec }
    }
}

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "KeyFilteringConfig", from_py_object)]
#[derive(Clone)]
pub struct PyKeyFilteringConfig {
    pub config: KeyFilteringConfig,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyKeyFilteringConfig {
    #[new]
    #[pyo3(signature = (num_workers=None, cpus_per_worker=None, keys_load_batch_size=None, max_concurrency_per_worker=None, filter_batch_size=None))]
    pub fn new(
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        filter_batch_size: Option<usize>,
    ) -> PyResult<Self> {
        Ok(Self {
            config: KeyFilteringConfig::new(
                num_workers,
                cpus_per_worker,
                keys_load_batch_size,
                max_concurrency_per_worker,
                filter_batch_size,
            )?,
        })
    }

    #[getter]
    pub fn left_key_columns(&self) -> Vec<String> {
        self.config.left_key_columns.clone()
    }

    #[getter]
    pub fn right_key_columns(&self) -> Vec<String> {
        self.config.right_key_columns.clone()
    }

    #[getter]
    pub fn num_workers(&self) -> Option<usize> {
        self.config.num_workers
    }

    #[getter]
    pub fn cpus_per_worker(&self) -> Option<f64> {
        self.config.cpus_per_worker.as_ref().map(|v| v.0)
    }

    #[getter]
    pub fn keys_load_batch_size(&self) -> Option<usize> {
        self.config.keys_load_batch_size
    }

    #[getter]
    pub fn max_concurrency_per_worker(&self) -> Option<usize> {
        self.config.max_concurrency_per_worker
    }

    #[getter]
    pub fn filter_batch_size(&self) -> Option<usize> {
        self.config.filter_batch_size
    }
}

#[cfg(feature = "python")]
impl From<KeyFilteringConfig> for PyKeyFilteringConfig {
    fn from(config: KeyFilteringConfig) -> Self {
        Self { config }
    }
}
