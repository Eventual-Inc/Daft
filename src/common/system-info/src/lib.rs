#[cfg(feature = "python")]
use pyo3::prelude::*;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};

#[cfg_attr(feature = "python", pyclass(module = "daft.daft", frozen))]
pub struct SystemInfo {
    info: sysinfo::System,
}

impl Default for SystemInfo {
    fn default() -> Self {
        SystemInfo {
            info: sysinfo::System::new_with_specifics(
                RefreshKind::new()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            ),
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl SystemInfo {
    #[new]
    pub fn new() -> Self {
        Default::default()
    }

    pub fn cpu_count(&self) -> Option<u64> {
        self.info.physical_core_count().map(|x| x as u64)
    }

    pub fn total_memory(&self) -> u64 {
        if let Some(cgroup) = self.info.cgroup_limits() {
            cgroup.total_memory
        } else {
            self.info.total_memory()
        }
    }
}

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<SystemInfo>()?;
    Ok(())
}
