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

    #[pyo3(signature = (default_io_config=None, enable_strict_filter_pushdown=None))]
    fn with_config_values(
        &mut self,
        default_io_config: Option<PyIOConfig>,
        enable_strict_filter_pushdown: Option<bool>,
    ) -> PyResult<Self> {
        let mut config = self.config.as_ref().clone();

        if let Some(default_io_config) = default_io_config {
            config.default_io_config = default_io_config.config;
        }

        if let Some(enable_strict_filter_pushdown) = enable_strict_filter_pushdown {
            config.enable_strict_filter_pushdown = enable_strict_filter_pushdown;
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

    #[getter(enable_strict_filter_pushdown)]
    fn enable_strict_filter_pushdown(&self) -> PyResult<bool> {
        Ok(self.config.enable_strict_filter_pushdown)
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
        enable_scan_task_split_and_merge=None,
        scan_tasks_min_size_bytes=None,
        scan_tasks_max_size_bytes=None,
        max_sources_per_scan_task=None,
        parquet_split_row_groups_max_files=None,
        broadcast_join_size_bytes_threshold=None,
        hash_join_partition_size_leniency=None,
        sample_size_for_sort=None,
        num_preview_rows=None,
        parquet_target_filesize=None,
        parquet_target_row_group_size=None,
        parquet_inflation_factor=None,
        csv_target_filesize=None,
        csv_inflation_factor=None,
        json_inflation_factor=None,
        shuffle_aggregation_default_partitions=None,
        partial_aggregation_threshold=None,
        high_cardinality_aggregation_threshold=None,
        read_sql_partition_size_bytes=None,
        default_morsel_size=None,
        shuffle_algorithm=None,
        pre_shuffle_merge_threshold=None,
        scantask_max_parallel=None,
        native_parquet_writer=None,
        min_cpu_per_task=None,
        actor_udf_ready_timeout=None,
        maintain_order=None,
        enable_dynamic_batching=None,
        dynamic_batching_strategy=None,
    ))]
    fn with_config_values(
        &self,
        enable_scan_task_split_and_merge: Option<bool>,
        scan_tasks_min_size_bytes: Option<usize>,
        scan_tasks_max_size_bytes: Option<usize>,
        max_sources_per_scan_task: Option<usize>,
        parquet_split_row_groups_max_files: Option<usize>,
        broadcast_join_size_bytes_threshold: Option<usize>,
        hash_join_partition_size_leniency: Option<f64>,
        sample_size_for_sort: Option<usize>,
        num_preview_rows: Option<usize>,
        parquet_target_filesize: Option<usize>,
        parquet_target_row_group_size: Option<usize>,
        parquet_inflation_factor: Option<f64>,
        csv_target_filesize: Option<usize>,
        csv_inflation_factor: Option<f64>,
        json_inflation_factor: Option<f64>,
        shuffle_aggregation_default_partitions: Option<usize>,
        partial_aggregation_threshold: Option<usize>,
        high_cardinality_aggregation_threshold: Option<f64>,
        read_sql_partition_size_bytes: Option<usize>,
        default_morsel_size: Option<usize>,
        shuffle_algorithm: Option<&str>,
        pre_shuffle_merge_threshold: Option<usize>,
        scantask_max_parallel: Option<usize>,
        native_parquet_writer: Option<bool>,
        min_cpu_per_task: Option<f64>,
        actor_udf_ready_timeout: Option<usize>,
        maintain_order: Option<bool>,
        enable_dynamic_batching: Option<bool>,
        dynamic_batching_strategy: Option<&str>,
    ) -> PyResult<Self> {
        let mut config = self.config.as_ref().clone();

        if let Some(enable_scan_task_split_and_merge) = enable_scan_task_split_and_merge {
            config.enable_scan_task_split_and_merge = enable_scan_task_split_and_merge;
        }
        if let Some(scan_tasks_min_size_bytes) = scan_tasks_min_size_bytes {
            config.scan_tasks_min_size_bytes = scan_tasks_min_size_bytes;
        }
        if let Some(scan_tasks_max_size_bytes) = scan_tasks_max_size_bytes {
            config.scan_tasks_max_size_bytes = scan_tasks_max_size_bytes;
        }
        if let Some(max_sources_per_scan_task) = max_sources_per_scan_task {
            config.max_sources_per_scan_task = max_sources_per_scan_task;
        }
        if let Some(parquet_split_row_groups_max_files) = parquet_split_row_groups_max_files {
            config.parquet_split_row_groups_max_files = parquet_split_row_groups_max_files;
        }
        if let Some(broadcast_join_size_bytes_threshold) = broadcast_join_size_bytes_threshold {
            config.broadcast_join_size_bytes_threshold = broadcast_join_size_bytes_threshold;
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
        if let Some(json_inflation_factor) = json_inflation_factor {
            config.json_inflation_factor = json_inflation_factor;
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

        if let Some(scantask_max_parallel) = scantask_max_parallel {
            config.scantask_max_parallel = scantask_max_parallel;
        }

        if let Some(native_parquet_writer) = native_parquet_writer {
            config.native_parquet_writer = native_parquet_writer;
        }

        if let Some(min_cpu_per_task) = min_cpu_per_task {
            config.min_cpu_per_task = min_cpu_per_task;
        }

        if let Some(actor_udf_ready_timeout) = actor_udf_ready_timeout {
            config.actor_udf_ready_timeout = actor_udf_ready_timeout;
        }

        if let Some(maintain_order) = maintain_order {
            config.maintain_order = maintain_order;
        }
        if let Some(enable_dynamic_batching) = enable_dynamic_batching {
            config.enable_dynamic_batching = enable_dynamic_batching;
        }
        if let Some(dynamic_batching_strategy) = dynamic_batching_strategy {
            if !matches!(dynamic_batching_strategy, "latency_constrained" | "auto") {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "dynamic_batching_strategy must be 'auto' or 'latency_constrained'",
                ));
            }
            config.dynamic_batching_strategy = dynamic_batching_strategy.to_string();
        }

        Ok(Self {
            config: Arc::new(config),
        })
    }

    #[getter]
    fn get_enable_scan_task_split_and_merge(&self) -> PyResult<bool> {
        Ok(self.config.enable_scan_task_split_and_merge)
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
    fn get_parquet_split_row_groups_max_files(&self) -> PyResult<usize> {
        Ok(self.config.parquet_split_row_groups_max_files)
    }

    fn get_broadcast_join_size_bytes_threshold(&self) -> PyResult<usize> {
        Ok(self.config.broadcast_join_size_bytes_threshold)
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
    fn get_json_inflation_factor(&self) -> PyResult<f64> {
        Ok(self.config.json_inflation_factor)
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
    fn scantask_max_parallel(&self) -> PyResult<usize> {
        Ok(self.config.scantask_max_parallel)
    }

    #[getter]
    fn min_cpu_per_task(&self) -> PyResult<f64> {
        Ok(self.config.min_cpu_per_task)
    }

    #[getter]
    fn actor_udf_ready_timeout(&self) -> PyResult<usize> {
        Ok(self.config.actor_udf_ready_timeout)
    }

    #[getter]
    fn maintain_order(&self) -> PyResult<bool> {
        Ok(self.config.maintain_order)
    }

    #[getter]
    fn enable_dynamic_batching(&self) -> PyResult<bool> {
        Ok(self.config.enable_dynamic_batching)
    }
    #[getter]
    fn dynamic_batching_strategy(&self) -> PyResult<&str> {
        Ok(self.config.dynamic_batching_strategy.as_str())
    }
}

impl_bincode_py_state_serialization!(PyDaftExecutionConfig);
