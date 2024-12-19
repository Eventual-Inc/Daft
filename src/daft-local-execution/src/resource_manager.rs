use std::sync::{Arc, Mutex, OnceLock};

use common_error::{DaftError, DaftResult};
use common_system_info::SystemInfo;
use lazy_static::lazy_static;
use tokio::sync::Notify;

lazy_static! {
    pub(crate) static ref MEMORY_MANAGER: OnceLock<Arc<MemoryManager>> = OnceLock::new();
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

pub(crate) fn get_or_init_memory_manager() -> &'static Arc<MemoryManager> {
    MEMORY_MANAGER.get_or_init(|| Arc::new(MemoryManager::new()))
}

pub(crate) struct MemoryPermit<'a> {
    bytes: u64,
    manager: &'a MemoryManager,
}

impl Drop for MemoryPermit<'_> {
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

pub(crate) struct MemoryManager {
    total_bytes: u64,
    state: Mutex<MemoryState>,
    notify: Notify,
}

impl Default for MemoryManager {
    fn default() -> Self {
        let system_info = SystemInfo::default();
        let total_mem = system_info.calculate_total_memory();
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

    pub async fn request_bytes(&self, bytes: u64) -> DaftResult<MemoryPermit> {
        if bytes == 0 {
            return Ok(MemoryPermit {
                bytes: 0,
                manager: self,
            });
        }

        if bytes > self.total_bytes {
            return Err(DaftError::ComputeError(format!(
                "Cannot request {} bytes, only {} available",
                bytes, self.total_bytes
            )));
        }

        loop {
            if let Some(permit) = self.try_request_bytes(bytes) {
                return Ok(permit);
            }
            self.notify.notified().await;
        }
    }

    fn try_request_bytes(&self, bytes: u64) -> Option<MemoryPermit> {
        let mut state = self.state.lock().unwrap();
        if state.available_bytes >= bytes {
            state.available_bytes -= bytes;
            Some(MemoryPermit {
                bytes,
                manager: self,
            })
        } else {
            None
        }
    }
}
