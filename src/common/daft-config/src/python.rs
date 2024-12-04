use std::sync::Arc;

use common_io_config::python::IOConfig as PyIOConfig;
use common_py_serde::impl_bincode_py_state_serialization;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{DaftExecutionConfig, DaftPlanningConfig};

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct PyDaftPlanningConfig {
    pub config: Arc<DaftPlanningConfig>,
}

#[pymethods]
impl PyDaftPlanningConfig {
    #[new]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[staticmethod]
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            config: Arc::new(DaftPlanningConfig::from_env()),
        }
    }

    fn with_config_values(
        &mut self,
        default_io_config: Option<PyIOConfig>,
        enable_actor_pool_projections: Option<bool>,
    ) -> PyResult<Self> {
        let mut config = self.config.as_ref().clone();

        if let Some(default_io_config) = default_io_config {
            config.default_io_config = default_io_config.config;
        }

        if let Some(enable_actor_pool_projections) = enable_actor_pool_projections {
            config.enable_actor_pool_projections = enable_actor_pool_projections;
        }

        Ok(Self {
            config: Arc::new(config),
        })
    }

    #[getter(default_io_config)]
    fn default_io_config(&self) -> PyResult<PyIOConfig> {
        Ok(PyIOConfig {
            config: self.config.default_io_config.clone(),
        })
    }

    #[getter(enable_actor_pool_projections)]
    fn enable_actor_pool_projections(&self) -> PyResult<bool> {
        Ok(self.config.enable_actor_pool_projections)
    }
}

impl_bincode_py_state_serialization!(PyDaftPlanningConfig);

#[derive(Clone, Default, Serialize, Deserialize)]
#[pyclass(module = "daft.daft")]
pub struct PyDaftExecutionConfig {
    pub config: Arc<DaftExecutionConfig>,
}

#[pymethods]
impl PyDaftExecutionConfig {
    #[new]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[staticmethod]
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            config: Arc::new(DaftExecutionConfig::from_env()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn with_config_values(
        &self,
        scan_tasks_min_size_bytes: Option<usize>,
        scan_tasks_max_size_bytes: Option<usize>,
        broadcast_join_size_bytes_threshold: Option<usize>,
        parquet_split_row_groups_max_files: Option<usize>,
        sort_merge_join_sort_with_aligned_boundaries: Option<bool>,
        hash_join_partition_size_leniency: Option<f64>,
        sample_size_for_sort: Option<usize>,
        num_preview_rows: Option<usize>,
        parquet_target_filesize: Option<usize>,
        parquet_target_row_group_size: Option<usize>,
        parquet_inflation_factor: Option<f64>,
        csv_target_filesize: Option<usize>,
        csv_inflation_factor: Option<f64>,
        shuffle_aggregation_default_partitions: Option<usize>,
        read_sql_partition_size_bytes: Option<usize>,
        enable_aqe: Option<bool>,
        enable_native_executor: Option<bool>,
        default_morsel_size: Option<usize>,
        shuffle_algorithm: Option<&str>,
        pre_shuffle_merge_threshold: Option<usize>,
        enable_ray_tracing: Option<bool>,
    ) -> PyResult<Self> {
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
        if let Some(hash_join_partition_size_leniency) = hash_join_partition_size_leniency {
            config.hash_join_partition_size_leniency = hash_join_partition_size_leniency;
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
        if let Some(read_sql_partition_size_bytes) = read_sql_partition_size_bytes {
            config.read_sql_partition_size_bytes = read_sql_partition_size_bytes;
        }

        if let Some(enable_aqe) = enable_aqe {
            config.enable_aqe = enable_aqe;
        }
        if let Some(enable_native_executor) = enable_native_executor {
            config.enable_native_executor = enable_native_executor;
        }
        if let Some(default_morsel_size) = default_morsel_size {
            config.default_morsel_size = default_morsel_size;
        }
        if let Some(shuffle_algorithm) = shuffle_algorithm {
            if !matches!(shuffle_algorithm, "map_reduce" | "pre_shuffle_merge") {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "shuffle_algorithm must be 'map_reduce' or 'pre_shuffle_merge'",
                ));
            }
            config.shuffle_algorithm = shuffle_algorithm.to_string();
        }
        if let Some(pre_shuffle_merge_threshold) = pre_shuffle_merge_threshold {
            config.pre_shuffle_merge_threshold = pre_shuffle_merge_threshold;
        }

        if let Some(enable_ray_tracing) = enable_ray_tracing {
            config.enable_ray_tracing = enable_ray_tracing;
        }

        Ok(Self {
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
    fn get_hash_join_partition_size_leniency(&self) -> PyResult<f64> {
        Ok(self.config.hash_join_partition_size_leniency)
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

    #[getter]
    fn get_read_sql_partition_size_bytes(&self) -> PyResult<usize> {
        Ok(self.config.read_sql_partition_size_bytes)
    }
    #[getter]
    fn enable_aqe(&self) -> PyResult<bool> {
        Ok(self.config.enable_aqe)
    }
    #[getter]
    fn enable_native_executor(&self) -> PyResult<bool> {
        Ok(self.config.enable_native_executor)
    }
    #[getter]
    fn default_morsel_size(&self) -> PyResult<usize> {
        Ok(self.config.default_morsel_size)
    }
    #[getter]
    fn shuffle_algorithm(&self) -> PyResult<&str> {
        Ok(self.config.shuffle_algorithm.as_str())
    }
    #[getter]
    fn pre_shuffle_merge_threshold(&self) -> PyResult<usize> {
        Ok(self.config.pre_shuffle_merge_threshold)
    }

    #[getter]
    fn enable_ray_tracing(&self) -> PyResult<bool> {
        Ok(self.config.enable_ray_tracing)
    }
}

impl_bincode_py_state_serialization!(PyDaftExecutionConfig);
