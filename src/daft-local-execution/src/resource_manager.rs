use std::sync::{Arc, Mutex};

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
            {
                let mut state = self.manager.state.lock().unwrap();
                state.available_bytes += self.bytes;
            } // lock is released here
            self.manager.notify.notify_waiters();
        }
    }
}

struct MemoryState {
    available_bytes: u64,
}

pub struct MemoryManager {
    total_bytes: u64,
    state: Mutex<MemoryState>,
    notify: Notify,
}

impl Default for MemoryManager {
    fn default() -> Self {
        let system_info = SystemInfoInternal::default();
        let total_mem = system_info.total_memory();
        Self {
            total_bytes: total_mem,
            state: Mutex::new(MemoryState {
                available_bytes: total_mem,
            }),
            notify: Notify::new(),
        }
    }
}

impl MemoryManager {
    pub fn new() -> Self {
        if let Some(custom_limit) = custom_memory_limit() {
            Self {
                total_bytes: custom_limit,
                state: Mutex::new(MemoryState {
                    available_bytes: custom_limit,
                }),
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
            let can_allocate = {
                let mut state = self.state.lock().unwrap();
                if state.available_bytes >= bytes {
                    state.available_bytes -= bytes;
                    true
                } else {
                    false
                }
            }; // lock is released here

            if can_allocate {
                return Ok(MemoryPermit {
                    bytes,
                    manager: self.clone(),
                });
            }

            self.notify.notified().await;
        }
    }
}
