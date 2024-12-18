use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use common_error::{DaftError, DaftResult};
use common_system_info::SystemInfoInternal;
use lazy_static::lazy_static;
use tokio::sync::Notify;

lazy_static! {
    pub static ref MEMORY_MANAGER: Arc<MemoryManager> = Arc::new(MemoryManager::new());
}

fn custom_memory_limit() -> Option<u64> {
    let memory_limit_var_name = "DAFT_MEMORY_LIMIT";
    if let Ok(val) = std::env::var(memory_limit_var_name) {
        if let Ok(val) = val.parse::<u64>() {
            return Some(val);
        }
    }
    None
}

pub fn get_memory_manager() -> Arc<MemoryManager> {
    MEMORY_MANAGER.clone()
}

pub struct MemoryPermit {
    bytes: u64,
    manager: Arc<MemoryManager>,
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.manager
                .available_bytes
                .fetch_add(self.bytes, Ordering::Release);
            self.manager.notify.notify_waiters();
        }
    }
}

pub struct MemoryManager {
    total_bytes: u64,
    available_bytes: AtomicU64,
    notify: Notify,
}

impl Default for MemoryManager {
    fn default() -> Self {
        let system_info = SystemInfoInternal::default();
        Self {
            total_bytes: system_info.total_memory(),
            available_bytes: AtomicU64::new(system_info.total_memory()),
            notify: Notify::new(),
        }
    }
}

impl MemoryManager {
    pub fn new() -> Self {
        if let Some(custom_limit) = custom_memory_limit() {
            Self {
                total_bytes: custom_limit,
                available_bytes: AtomicU64::new(custom_limit),
                notify: Notify::new(),
            }
        } else {
            Self::default()
        }
    }

    pub async fn request_bytes(self: &Arc<Self>, bytes: u64) -> DaftResult<MemoryPermit> {
        if bytes == 0 {
            return Err(DaftError::ComputeError(
                "Cannot request 0 bytes".to_string(),
            ));
        }
        if bytes > self.total_bytes {
            return Err(DaftError::ComputeError(format!(
                "Cannot request {} bytes, only {} available",
                bytes, self.total_bytes
            )));
        }

        loop {
            let available = self.available_bytes.load(Ordering::Acquire);
            if available >= bytes {
                // Try to atomically subtract the requested bytes
                match self.available_bytes.compare_exchange(
                    available,
                    available - bytes,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        return Ok(MemoryPermit {
                            bytes,
                            manager: self.clone(),
                        });
                    }
                    Err(_) => {
                        // Someone else modified the value, try again
                        continue;
                    }
                }
            }
            self.notify.notified().await;
        }
    }
}
