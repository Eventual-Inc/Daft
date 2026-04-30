use daft_common_error::{DaftError, DaftResult};
use common_hashable_float_wrapper::FloatWrapper;
use educe::Educe;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

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
    use super::KeyFilteringConfig;

    #[test]
    fn key_filtering_config_rejects_invalid_runtime_inputs() {
        assert!(KeyFilteringConfig::new(Some(0), Some(1.0), None, None, None).is_err());
        assert!(KeyFilteringConfig::new(Some(1), Some(0.0), None, None, None).is_err());
        assert!(KeyFilteringConfig::new(Some(1), Some(1.0), Some(0), None, None).is_err());
        assert!(KeyFilteringConfig::new(Some(1), Some(1.0), None, Some(0), None).is_err());
        assert!(KeyFilteringConfig::new(Some(1), Some(1.0), None, None, Some(0)).is_err());
    }

    #[test]
    fn key_filtering_config_requires_matching_non_empty_key_columns() {
        let config = KeyFilteringConfig::new(Some(1), Some(1.0), None, None, None).unwrap();

        assert!(
            config
                .clone()
                .with_key_columns(vec![], vec!["id".to_string()])
                .is_err()
        );
        assert!(
            config
                .clone()
                .with_key_columns(vec!["id".to_string()], vec![])
                .is_err()
        );
        assert!(
            config
                .with_key_columns(
                    vec!["id".to_string()],
                    vec!["id".to_string(), "id2".to_string()]
                )
                .is_err()
        );
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
