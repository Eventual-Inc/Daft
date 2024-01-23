use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

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
    pub merge_scan_tasks_min_size_bytes: usize,
    pub merge_scan_tasks_max_size_bytes: usize,
    pub broadcast_join_size_bytes_threshold: usize,
    pub sample_size_for_sort: usize,
    pub split_row_groups_max_files: usize,
    pub num_preview_rows: usize,
}

impl Default for DaftExecutionConfig {
    fn default() -> Self {
        DaftExecutionConfig {
            merge_scan_tasks_min_size_bytes: 64 * 1024 * 1024, // 64MB
            merge_scan_tasks_max_size_bytes: 512 * 1024 * 1024, // 512MB
            broadcast_join_size_bytes_threshold: 10 * 1024 * 1024, // 10 MiB
            sample_size_for_sort: 20,
            split_row_groups_max_files: 10,
            num_preview_rows: 8,
        }
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
