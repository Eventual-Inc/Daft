#![feature(let_chains)]
pub use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

/// Configurations for Daft to use during the building of a Dataframe's plan.
///
/// 1. Creation of a Dataframe including any file listing and schema inference that needs to happen. Note
///    that this does not include the actual scan, which is taken care of by the DaftExecutionConfig.
/// 2. Building of logical plan nodes
#[derive(Clone, Serialize, Deserialize, Default, Debug, Eq, PartialEq)]
pub struct DaftPlanningConfig {
    pub default_io_config: IOConfig,
    pub disable_join_reordering: bool,
}

impl DaftPlanningConfig {
    #[must_use]
    pub fn from_env() -> Self {
        let mut cfg: Self = Default::default();
        let disable_join_reordering_var_name = "DAFT_DEV_DISABLE_JOIN_REORDERING";
        if let Ok(val) = std::env::var(disable_join_reordering_var_name)
            && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
        {
            cfg.disable_join_reordering = true;
        }
        cfg
    }
}

/// Configurations for Daft to use during the execution of a Dataframe
///  Note that this should be immutable for a given end-to-end execution of a logical plan.
///
/// Execution entails everything that happens when a Dataframe `.collect()`, `.show()` or similar is called:
/// 1. Logical plan optimization
/// 2. Logical-to-physical-plan translation
/// 3. Task generation from physical plan
/// 4. Task scheduling
/// 5. Task local execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaftExecutionConfig {
    pub scan_tasks_min_size_bytes: usize,
    pub scan_tasks_max_size_bytes: usize,
    pub max_sources_per_scan_task: usize,
    pub broadcast_join_size_bytes_threshold: usize,
    pub sort_merge_join_sort_with_aligned_boundaries: bool,
    pub hash_join_partition_size_leniency: f64,
    pub sample_size_for_sort: usize,
    pub parquet_split_row_groups_max_files: usize,
    pub num_preview_rows: usize,
    pub parquet_target_filesize: usize,
    pub parquet_target_row_group_size: usize,
    pub parquet_inflation_factor: f64,
    pub csv_target_filesize: usize,
    pub csv_inflation_factor: f64,
    pub json_target_filesize: usize,
    pub json_inflation_factor: f64,
    pub shuffle_aggregation_default_partitions: usize,
    pub partial_aggregation_threshold: usize,
    pub high_cardinality_aggregation_threshold: f64,
    pub read_sql_partition_size_bytes: usize,
    pub enable_aqe: bool,
    pub default_morsel_size: usize,
    pub shuffle_algorithm: String,
    pub pre_shuffle_merge_threshold: usize,
    pub flight_shuffle_dirs: Vec<String>,
    pub enable_ray_tracing: bool,
    pub scantask_splitting_level: i32,
    pub native_parquet_writer: bool,
    pub use_experimental_distributed_engine: bool,
    pub min_cpu_per_task: f64,
}

impl Default for DaftExecutionConfig {
    fn default() -> Self {
        Self {
            scan_tasks_min_size_bytes: 96 * 1024 * 1024,  // 96MB
            scan_tasks_max_size_bytes: 384 * 1024 * 1024, // 384MB
            max_sources_per_scan_task: 10,
            broadcast_join_size_bytes_threshold: 10 * 1024 * 1024, // 10 MiB
            sort_merge_join_sort_with_aligned_boundaries: false,
            hash_join_partition_size_leniency: 0.5,
            sample_size_for_sort: 20,
            parquet_split_row_groups_max_files: 10,
            num_preview_rows: 8,
            parquet_target_filesize: 512 * 1024 * 1024, // 512MB
            parquet_target_row_group_size: 128 * 1024 * 1024, // 128MB
            parquet_inflation_factor: 3.0,
            csv_target_filesize: 512 * 1024 * 1024, // 512MB
            csv_inflation_factor: 0.5,
            json_target_filesize: 512 * 1024 * 1024, // 512MB
            json_inflation_factor: 0.5, // TODO(desmond): This can be tuned with more real world datasets.
            shuffle_aggregation_default_partitions: 200,
            partial_aggregation_threshold: 10000,
            high_cardinality_aggregation_threshold: 0.8,
            read_sql_partition_size_bytes: 512 * 1024 * 1024, // 512MB
            enable_aqe: false,
            default_morsel_size: 128 * 1024,
            shuffle_algorithm: "auto".to_string(),
            pre_shuffle_merge_threshold: 1024 * 1024 * 1024, // 1GB
            flight_shuffle_dirs: vec!["/tmp".to_string()],
            enable_ray_tracing: false,
            scantask_splitting_level: 1,
            native_parquet_writer: true,
            use_experimental_distributed_engine: true,
            min_cpu_per_task: 0.5,
        }
    }
}

impl DaftExecutionConfig {
    #[must_use]
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        let aqe_env_var_name = "DAFT_ENABLE_AQE";
        if let Ok(val) = std::env::var(aqe_env_var_name)
            && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
        {
            cfg.enable_aqe = true;
        }
        let ray_tracing_env_var_name = "DAFT_ENABLE_RAY_TRACING";
        if let Ok(val) = std::env::var(ray_tracing_env_var_name)
            && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
        {
            cfg.enable_ray_tracing = true;
        }
        let shuffle_algorithm_env_var_name = "DAFT_SHUFFLE_ALGORITHM";
        if let Ok(val) = std::env::var(shuffle_algorithm_env_var_name) {
            cfg.shuffle_algorithm = val;
        }
        let enable_aggressive_scantask_splitting_env_var_name = "DAFT_SCANTASK_SPLITTING_LEVEL";
        if let Ok(val) = std::env::var(enable_aggressive_scantask_splitting_env_var_name) {
            cfg.scantask_splitting_level = val.parse::<i32>().unwrap_or(0);
        }
        let native_parquet_writer_env_var_name = "DAFT_NATIVE_PARQUET_WRITER";
        if let Ok(val) = std::env::var(native_parquet_writer_env_var_name)
            && matches!(val.trim().to_lowercase().as_str(), "0" | "false")
        {
            cfg.native_parquet_writer = false;
        }
        let flotilla_env_var_name = "DAFT_FLOTILLA";
        if let Ok(val) = std::env::var(flotilla_env_var_name) {
            cfg.use_experimental_distributed_engine =
                !matches!(val.trim().to_lowercase().as_str(), "0" | "false");
        }
        let min_cpu_var = "DAFT_MIN_CPU_PER_TASK";
        if let Ok(val) = std::env::var(min_cpu_var) {
            match val.parse::<f64>() {
                Ok(parsed) => cfg.min_cpu_per_task = parsed,
                Err(_) => eprintln!(
                    "Invalid {} value: {}, using default {}",
                    min_cpu_var, val, cfg.min_cpu_per_task
                ),
            }
        }
        cfg
    }
}

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pub use python::PyDaftExecutionConfig;
#[cfg(feature = "python")]
pub use python::PyDaftPlanningConfig;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PyDaftExecutionConfig>()?;
    parent.add_class::<python::PyDaftPlanningConfig>()?;

    Ok(())
}
