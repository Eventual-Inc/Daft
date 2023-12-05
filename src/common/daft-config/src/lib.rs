#[derive(Clone)]
pub struct DaftConfig {
    merge_scan_tasks_min_size_bytes: usize,
    merge_scan_tasks_max_size_bytes: usize,
}

impl Default for DaftConfig {
    fn default() -> Self {
        DaftConfig {
            merge_scan_tasks_min_size_bytes: 64 * 1024 * 1024, // 64MB
            merge_scan_tasks_max_size_bytes: 512 * 1024 * 1024, // 512MB
        }
    }
}

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub use python::PyDaftConfig;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<python::PyDaftConfig>()?;

    Ok(())
}
