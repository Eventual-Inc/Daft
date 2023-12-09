use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct DaftExecutionConfig {
    pub merge_scan_tasks_min_size_bytes: usize,
    pub merge_scan_tasks_max_size_bytes: usize,
    pub broadcast_join_size_bytes_threshold: usize,
}

impl Default for DaftExecutionConfig {
    fn default() -> Self {
        DaftExecutionConfig {
            merge_scan_tasks_min_size_bytes: 64 * 1024 * 1024, // 64MB
            merge_scan_tasks_max_size_bytes: 512 * 1024 * 1024, // 512MB
            broadcast_join_size_bytes_threshold: 10 * 1024 * 1024, // 10 MiB
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

    Ok(())
}
