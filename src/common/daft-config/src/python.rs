use std::sync::Arc;

use pyo3::{prelude::*, PyTypeInfo};
use serde::{Deserialize, Serialize};

use crate::{DaftExecutionConfig, DaftPlanningConfig};
use common_io_config::python::IOConfig as PyIOConfig;

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct PyDaftPlanningConfig {
    pub config: Arc<DaftPlanningConfig>,
}

#[pymethods]
impl PyDaftPlanningConfig {
    #[new]
    pub fn new() -> Self {
        PyDaftPlanningConfig::default()
    }

    fn with_config_values(
        &mut self,
        default_io_config: Option<PyIOConfig>,
    ) -> PyResult<PyDaftPlanningConfig> {
        let mut config = self.config.as_ref().clone();

        if let Some(default_io_config) = default_io_config {
            config.default_io_config = default_io_config.config;
        }

        Ok(PyDaftPlanningConfig {
            config: Arc::new(config),
        })
    }

    #[getter(default_io_config)]
    fn default_io_config(&self) -> PyResult<PyIOConfig> {
        Ok(PyIOConfig {
            config: self.config.default_io_config.clone(),
        })
    }

    fn __reduce__(&self, py: Python) -> PyResult<(PyObject, (Vec<u8>,))> {
        let bin_data = bincode::serialize(self.config.as_ref())
            .expect("DaftPlanningConfig should be serializable to bytes");
        Ok((
            Self::type_object(py)
                .getattr("_from_serialized")?
                .to_object(py),
            (bin_data,),
        ))
    }

    #[staticmethod]
    fn _from_serialized(bin_data: Vec<u8>) -> PyResult<PyDaftPlanningConfig> {
        let daft_planning_config: DaftPlanningConfig = bincode::deserialize(bin_data.as_slice())
            .expect("DaftExecutionConfig should be deserializable from bytes");
        Ok(PyDaftPlanningConfig {
            config: daft_planning_config.into(),
        })
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct PyDaftExecutionConfig {
    pub config: Arc<DaftExecutionConfig>,
}

#[pymethods]
impl PyDaftExecutionConfig {
    #[new]
    pub fn new() -> Self {
        PyDaftExecutionConfig::default()
    }

    fn with_config_values(
        &self,
        merge_scan_tasks_min_size_bytes: Option<usize>,
        merge_scan_tasks_max_size_bytes: Option<usize>,
        broadcast_join_size_bytes_threshold: Option<usize>,
        split_row_groups_max_files: Option<usize>,
        split_row_groups_threshold_bytes: Option<usize>,
        split_row_groups_min_size_bytes: Option<usize>,
    ) -> PyResult<PyDaftExecutionConfig> {
        let mut config = self.config.as_ref().clone();

        if let Some(merge_scan_tasks_max_size_bytes) = merge_scan_tasks_max_size_bytes {
            config.merge_scan_tasks_max_size_bytes = merge_scan_tasks_max_size_bytes;
        }
        if let Some(merge_scan_tasks_min_size_bytes) = merge_scan_tasks_min_size_bytes {
            config.merge_scan_tasks_min_size_bytes = merge_scan_tasks_min_size_bytes;
        }
        if let Some(broadcast_join_size_bytes_threshold) = broadcast_join_size_bytes_threshold {
            config.broadcast_join_size_bytes_threshold = broadcast_join_size_bytes_threshold;
        }
        if let Some(split_row_groups_max_files) = split_row_groups_max_files {
            config.split_row_groups_max_files = split_row_groups_max_files
        }
        if let Some(split_row_groups_threshold_bytes) = split_row_groups_threshold_bytes {
            config.split_row_groups_threshold_bytes = split_row_groups_threshold_bytes
        }
        if let Some(split_row_groups_min_size_bytes) = split_row_groups_min_size_bytes {
            config.split_row_groups_min_size_bytes = split_row_groups_min_size_bytes
        }

        Ok(PyDaftExecutionConfig {
            config: Arc::new(config),
        })
    }

    #[getter]
    fn get_merge_scan_tasks_min_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.merge_scan_tasks_min_size_bytes)
    }

    #[getter]
    fn get_merge_scan_tasks_max_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.merge_scan_tasks_max_size_bytes)
    }

    #[getter]
    fn get_broadcast_join_size_bytes_threshold(&self) -> PyResult<usize> {
        Ok(self.config.broadcast_join_size_bytes_threshold)
    }

    #[getter]
    fn get_sample_size_for_sort(&self) -> PyResult<usize> {
        Ok(self.config.sample_size_for_sort)
    }

    #[getter]
    fn get_num_preview_rows(&self) -> PyResult<usize> {
        Ok(self.config.num_preview_rows)
    }

    fn __reduce__(&self, py: Python) -> PyResult<(PyObject, (Vec<u8>,))> {
        let bin_data = bincode::serialize(self.config.as_ref())
            .expect("DaftExecutionConfig should be serializable to bytes");
        Ok((
            Self::type_object(py)
                .getattr("_from_serialized")?
                .to_object(py),
            (bin_data,),
        ))
    }

    #[staticmethod]
    fn _from_serialized(bin_data: Vec<u8>) -> PyResult<PyDaftExecutionConfig> {
        let daft_execution_config: DaftExecutionConfig = bincode::deserialize(bin_data.as_slice())
            .expect("DaftExecutionConfig should be deserializable from bytes");
        Ok(PyDaftExecutionConfig {
            config: daft_execution_config.into(),
        })
    }
}
