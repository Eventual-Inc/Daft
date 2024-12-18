#[cfg(feature = "python")]
use pyo3::prelude::*;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};

pub struct SystemInfoInternal {
    info: sysinfo::System,
}

impl Default for SystemInfoInternal {
    fn default() -> Self {
        Self {
            info: sysinfo::System::new_with_specifics(
                RefreshKind::new()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            ),
        }
    }
}

impl SystemInfoInternal {
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

#[cfg_attr(feature = "python", pyclass(module = "daft.daft", frozen))]
#[derive(Default)]
pub struct SystemInfo {
    info: SystemInfoInternal,
}

#[cfg(feature = "python")]
#[pymethods]
impl SystemInfo {
    #[new]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn cpu_count(&self) -> Option<u64> {
        self.info.cpu_count()
    }

    #[must_use]
    pub fn total_memory(&self) -> u64 {
        self.info.total_memory()
    }
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<SystemInfo>()?;
    Ok(())
}
