use std::sync::Arc;

use pyo3::prelude::*;

use crate::DaftConfig;

#[derive(Clone, Default)]
#[pyclass]
pub struct PyDaftConfig {
    pub config: Arc<DaftConfig>,
}

#[pymethods]
impl PyDaftConfig {
    #[new]
    pub fn new() -> Self {
        PyDaftConfig::default()
    }

    fn with_config_values(
        &mut self,
        merge_scan_tasks_min_size_bytes: Option<usize>,
        merge_scan_tasks_max_size_bytes: Option<usize>,
    ) -> PyResult<PyDaftConfig> {
        let mut config = self.config.as_ref().clone();

        if let Some(merge_scan_tasks_max_size_bytes) = merge_scan_tasks_max_size_bytes {
            config.merge_scan_tasks_max_size_bytes = merge_scan_tasks_max_size_bytes;
        }
        if let Some(merge_scan_tasks_min_size_bytes) = merge_scan_tasks_min_size_bytes {
            config.merge_scan_tasks_min_size_bytes = merge_scan_tasks_min_size_bytes;
        }

        Ok(PyDaftConfig {
            config: Arc::new(config),
        })
    }

    #[getter(merge_scan_tasks_min_size_bytes)]
    fn get_merge_scan_tasks_min_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.merge_scan_tasks_min_size_bytes)
    }

    #[getter(merge_scan_tasks_max_size_bytes)]
    fn get_merge_scan_tasks_max_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.merge_scan_tasks_max_size_bytes)
    }
}
