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
    #[allow(clippy::too_many_arguments)]
    fn with_config_values(
        &self,
        scan_tasks_min_size_bytes: Option<usize>,
        scan_tasks_max_size_bytes: Option<usize>,
        broadcast_join_size_bytes_threshold: Option<usize>,
        parquet_split_row_groups_max_files: Option<usize>,
        sort_merge_join_sort_with_aligned_boundaries: Option<bool>,
        sample_size_for_sort: Option<usize>,
        num_preview_rows: Option<usize>,
        parquet_target_filesize: Option<usize>,
        parquet_target_row_group_size: Option<usize>,
        parquet_inflation_factor: Option<f64>,
        csv_target_filesize: Option<usize>,
        csv_inflation_factor: Option<f64>,
        shuffle_aggregation_default_partitions: Option<usize>,
    ) -> PyResult<PyDaftExecutionConfig> {
        let mut config = self.config.as_ref().clone();

        if let Some(scan_tasks_max_size_bytes) = scan_tasks_max_size_bytes {
            config.scan_tasks_max_size_bytes = scan_tasks_max_size_bytes;
        }
        if let Some(scan_tasks_min_size_bytes) = scan_tasks_min_size_bytes {
            config.scan_tasks_min_size_bytes = scan_tasks_min_size_bytes;
        }
        if let Some(broadcast_join_size_bytes_threshold) = broadcast_join_size_bytes_threshold {
            config.broadcast_join_size_bytes_threshold = broadcast_join_size_bytes_threshold;
        }
        if let Some(parquet_split_row_groups_max_files) = parquet_split_row_groups_max_files {
            config.parquet_split_row_groups_max_files = parquet_split_row_groups_max_files;
        }
        if let Some(sort_merge_join_sort_with_aligned_boundaries) =
            sort_merge_join_sort_with_aligned_boundaries
        {
            config.sort_merge_join_sort_with_aligned_boundaries =
                sort_merge_join_sort_with_aligned_boundaries;
        }
        if let Some(sample_size_for_sort) = sample_size_for_sort {
            config.sample_size_for_sort = sample_size_for_sort;
        }
        if let Some(num_preview_rows) = num_preview_rows {
            config.num_preview_rows = num_preview_rows;
        }
        if let Some(parquet_target_filesize) = parquet_target_filesize {
            config.parquet_target_filesize = parquet_target_filesize;
        }
        if let Some(parquet_target_row_group_size) = parquet_target_row_group_size {
            config.parquet_target_row_group_size = parquet_target_row_group_size;
        }
        if let Some(parquet_inflation_factor) = parquet_inflation_factor {
            config.parquet_inflation_factor = parquet_inflation_factor;
        }
        if let Some(csv_target_filesize) = csv_target_filesize {
            config.csv_target_filesize = csv_target_filesize;
        }
        if let Some(csv_inflation_factor) = csv_inflation_factor {
            config.csv_inflation_factor = csv_inflation_factor;
        }
        if let Some(shuffle_aggregation_default_partitions) = shuffle_aggregation_default_partitions
        {
            config.shuffle_aggregation_default_partitions = shuffle_aggregation_default_partitions;
        }

        Ok(PyDaftExecutionConfig {
            config: Arc::new(config),
        })
    }

    #[getter]
    fn get_scan_tasks_min_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.scan_tasks_min_size_bytes)
    }

    #[getter]
    fn get_scan_tasks_max_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.scan_tasks_max_size_bytes)
    }

    #[getter]
    fn get_broadcast_join_size_bytes_threshold(&self) -> PyResult<usize> {
        Ok(self.config.broadcast_join_size_bytes_threshold)
    }

    #[getter]
    fn get_sort_merge_join_sort_with_aligned_boundaries(&self) -> PyResult<bool> {
        Ok(self.config.sort_merge_join_sort_with_aligned_boundaries)
    }

    #[getter]
    fn get_sample_size_for_sort(&self) -> PyResult<usize> {
        Ok(self.config.sample_size_for_sort)
    }

    #[getter]
    fn get_num_preview_rows(&self) -> PyResult<usize> {
        Ok(self.config.num_preview_rows)
    }

    #[getter]
    fn get_parquet_target_filesize(&self) -> PyResult<usize> {
        Ok(self.config.parquet_target_filesize)
    }

    #[getter]
    fn get_parquet_target_row_group_size(&self) -> PyResult<usize> {
        Ok(self.config.parquet_target_row_group_size)
    }

    #[getter]
    fn get_parquet_inflation_factor(&self) -> PyResult<f64> {
        Ok(self.config.parquet_inflation_factor)
    }

    #[getter]
    fn get_csv_target_filesize(&self) -> PyResult<usize> {
        Ok(self.config.csv_target_filesize)
    }

    #[getter]
    fn get_csv_inflation_factor(&self) -> PyResult<f64> {
        Ok(self.config.csv_inflation_factor)
    }

    #[getter]
    fn get_shuffle_aggregation_default_partitions(&self) -> PyResult<usize> {
        Ok(self.config.shuffle_aggregation_default_partitions)
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
