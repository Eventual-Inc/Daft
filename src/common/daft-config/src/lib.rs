#![feature(let_chains)]
use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

/// Environment variables for Daft to use when formatting displays.
pub const BOLD_TABLE_HEADERS_IN_DISPLAY: &str = "DAFT_BOLD_TABLE_HEADERS";

/// Configurations for Daft to use during the building of a Dataframe's plan.
///
/// 1. Creation of a Dataframe including any file listing and schema inference that needs to happen. Note
///     that this does not include the actual scan, which is taken care of by the DaftExecutionConfig.
/// 2. Building of logical plan nodes
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct DaftPlanningConfig {
    pub default_io_config: IOConfig,
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
#[derive(Clone, Serialize, Deserialize)]
pub struct DaftExecutionConfig {
    pub scan_tasks_min_size_bytes: usize,
    pub scan_tasks_max_size_bytes: usize,
    pub broadcast_join_size_bytes_threshold: usize,
    pub sort_merge_join_sort_with_aligned_boundaries: bool,
    pub sample_size_for_sort: usize,
    pub parquet_split_row_groups_max_files: usize,
    pub num_preview_rows: usize,
    pub parquet_target_filesize: usize,
    pub parquet_target_row_group_size: usize,
    pub parquet_inflation_factor: f64,
    pub csv_target_filesize: usize,
    pub csv_inflation_factor: f64,
    pub shuffle_aggregation_default_partitions: usize,
    pub read_sql_partition_size_bytes: usize,
    pub enable_aqe: bool,
    pub enable_native_executor: bool,
}

impl Default for DaftExecutionConfig {
    fn default() -> Self {
        DaftExecutionConfig {
            scan_tasks_min_size_bytes: 96 * 1024 * 1024,  // 96MB
            scan_tasks_max_size_bytes: 384 * 1024 * 1024, // 384MB
            broadcast_join_size_bytes_threshold: 10 * 1024 * 1024, // 10 MiB
            sort_merge_join_sort_with_aligned_boundaries: false,
            sample_size_for_sort: 20,
            parquet_split_row_groups_max_files: 10,
            num_preview_rows: 8,
            parquet_target_filesize: 512 * 1024 * 1024, // 512MB
            parquet_target_row_group_size: 128 * 1024 * 1024, // 128MB
            parquet_inflation_factor: 3.0,
            csv_target_filesize: 512 * 1024 * 1024, // 512MB
            csv_inflation_factor: 0.5,
            shuffle_aggregation_default_partitions: 200,
            read_sql_partition_size_bytes: 512 * 1024 * 1024, // 512MB
            enable_aqe: false,
            enable_native_executor: false,
        }
    }
}

impl DaftExecutionConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        let aqe_env_var_name = "DAFT_ENABLE_AQE";
        if let Ok(val) = std::env::var(aqe_env_var_name)
            && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
        {
            cfg.enable_aqe = true;
        }
        let exec_env_var_name = "DAFT_ENABLE_NATIVE_EXECUTOR";
        if let Ok(val) = std::env::var(exec_env_var_name)
            && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
        {
            cfg.enable_native_executor = true;
        }
        cfg
    }
}

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub use python::PyDaftExecutionConfig;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<python::PyDaftExecutionConfig>()?;
    parent.add_class::<python::PyDaftPlanningConfig>()?;

    Ok(())
}
