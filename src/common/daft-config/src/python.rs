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

    #[pyo3(signature = (default_io_config=None))]
    fn with_config_values(&mut self, default_io_config: Option<PyIOConfig>) -> PyResult<Self> {
        let mut config = self.config.as_ref().clone();

        if let Some(default_io_config) = default_io_config {
            config.default_io_config = default_io_config.config;
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
    #[pyo3(signature = (
        scan_tasks_min_size_bytes=None,
        scan_tasks_max_size_bytes=None,
        max_sources_per_scan_task=None,
        broadcast_join_size_bytes_threshold=None,
        parquet_split_row_groups_max_files=None,
        sort_merge_join_sort_with_aligned_boundaries=None,
        hash_join_partition_size_leniency=None,
        sample_size_for_sort=None,
        num_preview_rows=None,
        parquet_target_filesize=None,
        parquet_target_row_group_size=None,
        parquet_inflation_factor=None,
        csv_target_filesize=None,
        csv_inflation_factor=None,
        shuffle_aggregation_default_partitions=None,
        partial_aggregation_threshold=None,
        high_cardinality_aggregation_threshold=None,
        read_sql_partition_size_bytes=None,
        enable_aqe=None,
        default_morsel_size=None,
        shuffle_algorithm=None,
        pre_shuffle_merge_threshold=None,
        flight_shuffle_dirs=None,
        enable_ray_tracing=None,
        scantask_splitting_level=None,
        native_parquet_writer=None,
        use_experimental_distributed_engine=None,
        min_cpu_per_task=None,
    ))]
    fn with_config_values(
        &self,
        scan_tasks_min_size_bytes: Option<usize>,
        scan_tasks_max_size_bytes: Option<usize>,
        max_sources_per_scan_task: Option<usize>,
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
        partial_aggregation_threshold: Option<usize>,
        high_cardinality_aggregation_threshold: Option<f64>,
        read_sql_partition_size_bytes: Option<usize>,
        enable_aqe: Option<bool>,
        default_morsel_size: Option<usize>,
        shuffle_algorithm: Option<&str>,
        pre_shuffle_merge_threshold: Option<usize>,
        flight_shuffle_dirs: Option<Vec<String>>,
        enable_ray_tracing: Option<bool>,
        scantask_splitting_level: Option<i32>,
        native_parquet_writer: Option<bool>,
        use_experimental_distributed_engine: Option<bool>,
        min_cpu_per_task: Option<f64>,
    ) -> PyResult<Self> {
        let mut config = self.config.as_ref().clone();

        if let Some(scan_tasks_max_size_bytes) = scan_tasks_max_size_bytes {
            config.scan_tasks_max_size_bytes = scan_tasks_max_size_bytes;
        }
        if let Some(scan_tasks_min_size_bytes) = scan_tasks_min_size_bytes {
            config.scan_tasks_min_size_bytes = scan_tasks_min_size_bytes;
        }
        if let Some(max_sources_per_scan_task) = max_sources_per_scan_task {
            config.max_sources_per_scan_task = max_sources_per_scan_task;
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
        if let Some(partial_aggregation_threshold) = partial_aggregation_threshold {
            config.partial_aggregation_threshold = partial_aggregation_threshold;
        }
        if let Some(high_cardinality_aggregation_threshold) = high_cardinality_aggregation_threshold
        {
            config.high_cardinality_aggregation_threshold = high_cardinality_aggregation_threshold;
        }
        if let Some(read_sql_partition_size_bytes) = read_sql_partition_size_bytes {
            config.read_sql_partition_size_bytes = read_sql_partition_size_bytes;
        }

        if let Some(enable_aqe) = enable_aqe {
            config.enable_aqe = enable_aqe;
        }
        if let Some(default_morsel_size) = default_morsel_size {
            config.default_morsel_size = default_morsel_size;
        }
        if let Some(shuffle_algorithm) = shuffle_algorithm {
            if !matches!(
                shuffle_algorithm,
                "map_reduce" | "pre_shuffle_merge" | "flight_shuffle" | "auto"
            ) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "shuffle_algorithm must be 'auto', 'map_reduce', 'pre_shuffle_merge', or 'flight_shuffle'",
                ));
            }
            config.shuffle_algorithm = shuffle_algorithm.to_string();
        }
        if let Some(pre_shuffle_merge_threshold) = pre_shuffle_merge_threshold {
            config.pre_shuffle_merge_threshold = pre_shuffle_merge_threshold;
        }
        if let Some(flight_shuffle_dirs) = flight_shuffle_dirs {
            config.flight_shuffle_dirs = flight_shuffle_dirs;
        }

        if let Some(enable_ray_tracing) = enable_ray_tracing {
            config.enable_ray_tracing = enable_ray_tracing;
        }

        if let Some(scantask_splitting_level) = scantask_splitting_level {
            if !matches!(scantask_splitting_level, 1 | 2) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "scantask_splitting_level must be 1 or 2",
                ));
            }
            config.scantask_splitting_level = scantask_splitting_level;
        }

        if let Some(native_parquet_writer) = native_parquet_writer {
            config.native_parquet_writer = native_parquet_writer;
        }

        if let Some(use_experimental_distributed_engine) = use_experimental_distributed_engine {
            config.use_experimental_distributed_engine = use_experimental_distributed_engine;
        }

        if let Some(min_cpu_per_task) = min_cpu_per_task {
            config.min_cpu_per_task = min_cpu_per_task;
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
    fn get_max_sources_per_scan_task(&self) -> PyResult<usize> {
        Ok(self.config.max_sources_per_scan_task)
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
    fn get_partial_aggregation_threshold(&self) -> PyResult<usize> {
        Ok(self.config.partial_aggregation_threshold)
    }

    #[getter]
    fn get_high_cardinality_aggregation_threshold(&self) -> PyResult<f64> {
        Ok(self.config.high_cardinality_aggregation_threshold)
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

    #[getter]
    fn scantask_splitting_level(&self) -> PyResult<i32> {
        Ok(self.config.scantask_splitting_level)
    }

    #[getter]
    fn use_experimental_distributed_engine(&self) -> PyResult<bool> {
        Ok(self.config.use_experimental_distributed_engine)
    }

    #[getter]
    fn min_cpu_per_task(&self) -> PyResult<f64> {
        Ok(self.config.min_cpu_per_task)
    }
}

impl_bincode_py_state_serialization!(PyDaftExecutionConfig);
