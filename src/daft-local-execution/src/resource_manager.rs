use std::sync::{Arc, Mutex, OnceLock};

use common_error::{DaftError, DaftResult};
use common_system_info::SystemInfo;
use tokio::sync::Notify;

pub(crate) static MEMORY_MANAGER: OnceLock<Arc<MemoryManager>> = OnceLock::new();

fn custom_memory_limit() -> Option<u64> {
    let memory_limit_var_name = "DAFT_MEMORY_LIMIT";
    if let Ok(val) = std::env::var(memory_limit_var_name)
        && let Ok(val) = val.parse::<u64>()
    {
        return Some(val);
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

/// A resident reservation against the spill pool, held for as long as an operator keeps data in
/// memory. Unlike `MemoryPermit` (released at task end), this is held across many `sink()` calls and
/// released at finalize or on drop. Bytes are returned to the pool on `Drop` on every path.
pub(crate) struct MemoryReservation {
    held: u64,
    manager: Arc<MemoryManager>,
}

impl MemoryReservation {
    /// Try to charge `bytes` more. Returns false (no error) if the pool is exhausted.
    pub fn try_grow(&mut self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        if self.manager.try_take_spill(bytes) {
            self.held += bytes;
            true
        } else {
            false
        }
    }

    /// Return up to `bytes` to the pool (clamped to what is held).
    pub fn shrink(&mut self, bytes: u64) {
        let b = bytes.min(self.held);
        self.held -= b;
        self.manager.give_back_spill(b);
    }

    pub fn held(&self) -> u64 {
        self.held
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.manager.give_back_spill(self.held);
    }
}

/// Implemented by blocking-sink states that hold spillable hash buckets so the shared
/// `reconcile_reservation` loop can drive spill decisions.
pub(crate) trait SpillableBuckets {
    /// Total in-memory bytes currently held across all buckets.
    fn resident_bytes(&self) -> u64;
    /// Spill the single largest in-memory bucket to disk, clearing it from memory. Returns false if
    /// there is nothing left to spill.
    fn spill_largest_bucket(&mut self) -> DaftResult<bool>;
}

/// Charge `state`'s resident bytes against the spill pool, spilling its largest buckets until the
/// pool admits the total. `cap` (per-operator deprecated cap) forces spilling once `held` would
/// exceed it even if the pool has room. Never blocks: if nothing can be spilled, returns with a
/// bounded (one-morsel) overshoot.
pub(crate) fn reconcile_reservation<S: SpillableBuckets>(
    state: &mut S,
    reservation: &mut MemoryReservation,
    cap: Option<u64>,
) -> DaftResult<()> {
    loop {
        let resident = state.resident_bytes();
        let held = reservation.held();
        if held > resident {
            reservation.shrink(held - resident);
            continue;
        }
        let need = resident - held;
        if need == 0 {
            return Ok(());
        }
        let over_cap = cap.is_some_and(|c| held + need > c);
        if !over_cap && reservation.try_grow(need) {
            return Ok(());
        }
        if !state.spill_largest_bucket()? {
            return Ok(()); // nothing left to spill → bounded overshoot
        }
    }
}

struct MemoryState {
    /// Transient working-memory budget (UDF reservations via `request_bytes`).
    available_bytes: u64,
    /// Total size of the resident spill pool (configurable).
    spill_pool_bytes: u64,
    /// Bytes of the spill pool currently held by live `MemoryReservation`s.
    spill_used_bytes: u64,
}

/// Default spill pool: 30% of the engine budget, floored at 64 MiB, overridable via env.
fn default_spill_pool_bytes(total: u64) -> u64 {
    const SPILL_FRACTION: f64 = 0.3;
    const MIN_POOL_BYTES: u64 = 64 * 1024 * 1024;
    if let Ok(val) = std::env::var("DAFT_SPILL_POOL_BYTES")
        && let Ok(parsed) = val.trim().parse::<u64>()
        && parsed > 0
    {
        return parsed;
    }
    (((total as f64) * SPILL_FRACTION) as u64).max(MIN_POOL_BYTES)
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
                spill_pool_bytes: default_spill_pool_bytes(total_mem),
                spill_used_bytes: 0,
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
                    spill_pool_bytes: default_spill_pool_bytes(custom_limit),
                    spill_used_bytes: 0,
                }),
                notify: Notify::new(),
            }
        } else {
            Self::default()
        }
    }

    /// Total memory budget (bytes) the engine is allowed to use, from `DAFT_MEMORY_LIMIT` or
    /// system memory. Used to auto-derive spill thresholds for blocking sinks.
    #[allow(dead_code)]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Current configured size of the resident spill pool (bytes).
    pub fn spill_pool_bytes(&self) -> u64 {
        self.state.lock().unwrap().spill_pool_bytes
    }

    /// Bytes of the spill pool currently held across all live reservations (diagnostics).
    #[allow(dead_code)]
    pub fn spill_used_bytes(&self) -> u64 {
        self.state.lock().unwrap().spill_used_bytes
    }

    /// Override the spill pool size (last-writer-wins; called at pipeline build from config).
    pub fn set_spill_pool_bytes(&self, n: u64) {
        let mut state = self.state.lock().unwrap();
        state.spill_pool_bytes = n;
        drop(state);
        self.notify.notify_waiters();
    }

    /// Create an empty resident reservation against the spill pool.
    pub fn reservation(self: &Arc<Self>) -> MemoryReservation {
        MemoryReservation {
            held: 0,
            manager: self.clone(),
        }
    }

    /// Try to charge `bytes` to the spill pool. Non-blocking; returns false if the pool is full.
    fn try_take_spill(&self, bytes: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.spill_used_bytes + bytes <= state.spill_pool_bytes {
            state.spill_used_bytes += bytes;
            true
        } else {
            false
        }
    }

    /// Return `bytes` to the spill pool.
    fn give_back_spill(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        {
            let mut state = self.state.lock().unwrap();
            state.spill_used_bytes = state.spill_used_bytes.saturating_sub(bytes);
        }
        self.notify.notify_waiters();
    }

    pub async fn request_bytes(&self, bytes: u64) -> DaftResult<MemoryPermit<'_>> {
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

    fn try_request_bytes(&self, bytes: u64) -> Option<MemoryPermit<'_>> {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time;

    use super::*;

    #[test]
    fn test_get_or_init_memory_manager() {
        let manager1 = get_or_init_memory_manager();
        let manager2 = get_or_init_memory_manager();

        // Verify we get the same instance
        assert!(Arc::ptr_eq(manager1, manager2));
    }

    #[tokio::test]
    async fn test_zero_byte_request() {
        let manager = MemoryManager::new();
        let permit = manager.request_bytes(0).await.unwrap();
        assert_eq!(permit.bytes, 0);
    }

    #[tokio::test]
    async fn test_excessive_memory_request() {
        let manager = MemoryManager::new();
        let result = manager.request_bytes(manager.total_bytes + 1).await;
        assert!(result.is_err());
        if let Err(DaftError::ComputeError(_)) = result {
            // Expected error type
        } else {
            panic!("Expected ComputeError");
        }
    }

    #[tokio::test]
    async fn test_successful_memory_request() {
        let manager = MemoryManager::new();
        let total = manager.total_bytes;
        let first_request_size = total / 2;
        let second_request_size = total - first_request_size;

        // First request should succeed
        let permit1 = manager.request_bytes(first_request_size).await.unwrap();
        assert_eq!(permit1.bytes, first_request_size);

        // Second request should succeed
        let permit2 = manager.request_bytes(second_request_size).await.unwrap();
        assert_eq!(permit2.bytes, second_request_size);

        // Third request should fail
        let result = manager.try_request_bytes(1);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_memory_release() {
        let manager = MemoryManager::new();
        let request_size = 1;
        let permit = manager.request_bytes(request_size).await.unwrap();

        // Verify available memory is reduced
        {
            let state = manager.state.lock().unwrap();
            assert_eq!(state.available_bytes, manager.total_bytes - request_size);
        }

        // Drop the permit
        drop(permit);

        // Verify memory is released
        {
            let state = manager.state.lock().unwrap();
            assert_eq!(state.available_bytes, manager.total_bytes);
        }
    }

    #[tokio::test]
    async fn test_waiting_for_memory() {
        let manager = Arc::new(MemoryManager::new());
        let total = manager.total_bytes;

        // Request all available memory
        let permit = manager.request_bytes(total).await.unwrap();

        // Spawn a task that waits for memory
        let manager_clone = manager.clone();
        let wait_handle = tokio::spawn(async move {
            let _permit = manager_clone.request_bytes(total / 2).await.unwrap();
        });

        // Short delay to ensure the waiting task is actually waiting
        time::sleep(Duration::from_millis(50)).await;

        // Drop the original permit
        drop(permit);

        // The waiting task should now complete
        wait_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_memory_requests() {
        let manager = Arc::new(MemoryManager::new());
        let total = manager.total_bytes;
        let mut task_set = tokio::task::JoinSet::new();

        // Four tasks that request all available memory
        for _ in 0..4 {
            let manager_clone = manager.clone();
            task_set.spawn(async move {
                let _permit = manager_clone.request_bytes(total).await.unwrap();
            });
        }

        // Four tasks that request half the available memory
        for _ in 0..4 {
            let manager_clone = manager.clone();
            task_set.spawn(async move {
                let _permit = manager_clone.request_bytes(total / 2).await.unwrap();
            });
        }

        // Four tasks that request a quarter of the available memory
        for _ in 0..4 {
            let manager_clone = manager.clone();
            task_set.spawn(async move {
                let _permit = manager_clone.request_bytes(total / 4).await.unwrap();
            });
        }

        task_set.join_all().await;
    }

    #[test]
    fn test_spill_pool_default_is_fraction_of_total() {
        // The default formula only applies when the env override is unset.
        if std::env::var("DAFT_SPILL_POOL_BYTES").is_ok() {
            return;
        }
        let manager = MemoryManager::new();
        // Default pool is 0.3 of total, floored at 64 MiB.
        let expected =
            ((manager.total_bytes as f64 * 0.3) as u64).max(64 * 1024 * 1024);
        assert_eq!(manager.spill_pool_bytes(), expected);
    }

    #[test]
    fn test_reservation_grow_shrink_and_drop_release() {
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(1000);
        let mut r = manager.reservation();
        assert!(r.try_grow(600));
        assert_eq!(r.held(), 600);
        assert!(r.try_grow(400)); // exactly fills pool
        assert!(!r.try_grow(1)); // pool exhausted
        r.shrink(500);
        assert_eq!(r.held(), 500);
        assert!(r.try_grow(500)); // room again after shrink
        drop(r);
        // After drop, a fresh reservation can take the whole pool back.
        let mut r2 = manager.reservation();
        assert!(r2.try_grow(1000));
    }

    #[test]
    fn test_reconcile_spills_largest_until_it_fits() {
        struct Fake {
            buckets: Vec<u64>,
            spilled: Vec<usize>,
        }
        impl SpillableBuckets for Fake {
            fn resident_bytes(&self) -> u64 {
                self.buckets.iter().sum()
            }
            fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
                let Some((idx, _)) = self
                    .buckets
                    .iter()
                    .enumerate()
                    .filter(|(_, b)| **b > 0)
                    .max_by_key(|(_, b)| **b)
                else {
                    return Ok(false);
                };
                self.spilled.push(idx);
                self.buckets[idx] = 0;
                Ok(true)
            }
        }
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(100);
        let mut r = manager.reservation();
        // Three buckets totalling 150 > pool 100 → must spill the largest (80).
        let mut f = Fake {
            buckets: vec![80, 40, 30],
            spilled: vec![],
        };
        reconcile_reservation(&mut f, &mut r, None).unwrap();
        assert_eq!(f.spilled, vec![0]); // largest spilled first
        assert_eq!(f.resident_bytes(), 70); // 40 + 30 remain
        assert_eq!(r.held(), 70); // exactly charged
    }

    #[test]
    fn test_spill_used_bytes_tracks_reservations() {
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(1000);
        let mut r = manager.reservation();
        assert_eq!(manager.spill_used_bytes(), 0);
        assert!(r.try_grow(300));
        assert_eq!(manager.spill_used_bytes(), 300);
        drop(r);
        assert_eq!(manager.spill_used_bytes(), 0);
    }

    #[test]
    fn test_reconcile_overshoot_when_nothing_to_spill() {
        struct Empty;
        impl SpillableBuckets for Empty {
            fn resident_bytes(&self) -> u64 {
                500
            }
            fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
                Ok(false) // nothing spillable
            }
        }
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(100);
        let mut r = manager.reservation();
        // resident 500 > pool 100, nothing to spill → returns Ok, bounded overshoot.
        reconcile_reservation(&mut Empty, &mut r, None).unwrap();
        // Nothing was spillable and the pool (100) cannot fit resident (500), so nothing is
        // charged — the data stays resident uncharged (bounded overshoot), held remains 0.
        assert_eq!(r.held(), 0);
    }
}
