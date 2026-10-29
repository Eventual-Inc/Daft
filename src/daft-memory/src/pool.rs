use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use thiserror::Error;
use tokio::sync::Notify;

use crate::release::{MemoryReleaseTarget, WeakReleaseTarget};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub(crate) struct ManagerState {
    accounting_lock: Mutex<()>,
    notify: Arc<Notify>,
    release_targets: Mutex<BTreeMap<u64, WeakReleaseTarget>>,
    released_bytes: AtomicU64,
    release_failures: AtomicU64,
}

impl ManagerState {
    pub(crate) fn new() -> Self {
        Self {
            accounting_lock: Mutex::new(()),
            notify: Arc::new(Notify::new()),
            release_targets: Mutex::new(BTreeMap::new()),
            released_bytes: AtomicU64::new(0),
            release_failures: AtomicU64::new(0),
        }
    }

    pub(crate) fn register_release_target(&self, id: u64, target: WeakReleaseTarget) {
        self.release_targets.lock().unwrap().insert(id, target);
    }

    pub(crate) fn unregister_release_target(&self, id: u64) {
        self.release_targets.lock().unwrap().remove(&id);
    }

    pub(crate) fn record_release(&self, released_bytes: u64) {
        self.released_bytes
            .fetch_add(released_bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_release_failure(&self) {
        self.release_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn released_bytes(&self) -> u64 {
        self.released_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn release_failures(&self) -> u64 {
        self.release_failures.load(Ordering::Relaxed)
    }

    pub(crate) fn notify_waiters(&self) {
        self.notify.notify_waiters();
    }

    fn request_release(self: &Arc<Self>, target_bytes: u64) {
        let mut targets = {
            let registered = self.release_targets.lock().unwrap();
            registered
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        targets.sort_unstable_by_key(|target| std::cmp::Reverse(target.reclaim_bytes()));

        let mut remaining = target_bytes;
        for target in targets {
            let available = target.claim_reclaim_bytes();
            if available == 0 {
                continue;
            }
            let requested = remaining.min(available);
            let _result = target.request(requested, self.notify.clone(), Arc::downgrade(self));
            remaining = remaining.saturating_sub(requested);
            if remaining == 0 {
                break;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryPoolKind {
    Worker,
    Spill,
    Pipeline,
    NodeShared,
    Input,
    Operator,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum MemoryError {
    #[error(
        "spill memory reserve of {reserved} bytes must be smaller than worker memory limit of {limit} bytes"
    )]
    InvalidSpillReserve { reserved: u64, limit: u64 },
    #[error("memory request of {requested} bytes exceeds the {pool} pool limit of {limit} bytes")]
    RequestExceedsPoolLimit {
        requested: u64,
        pool: String,
        limit: u64,
    },
    #[error("memory accounting overflow")]
    AccountingOverflow,
}

/// A node in Daft's worker -> pipeline accounting tree.
pub struct MemoryPool {
    id: u64,
    name: Arc<str>,
    kind: MemoryPoolKind,
    limit_bytes: AtomicU64,
    used_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    parent: Option<Arc<Self>>,
    manager: Arc<ManagerState>,
    // An isolated suballocation pool can be backed by a reservation in another pool.
    // Its entire budget stays charged there until all suballocations have been dropped.
    _backing: Option<MemoryPermit>,
}

impl fmt::Debug for MemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPool")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("limit_bytes", &self.limit_bytes())
            .field("used_bytes", &self.used_bytes())
            .field("peak_bytes", &self.peak_bytes())
            .finish_non_exhaustive()
    }
}

impl MemoryPool {
    pub(crate) fn new_root(
        manager: Arc<ManagerState>,
        name: impl Into<Arc<str>>,
        kind: MemoryPoolKind,
        limit_bytes: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            name: name.into(),
            kind,
            limit_bytes: AtomicU64::new(limit_bytes),
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            parent: None,
            manager,
            _backing: None,
        })
    }

    #[must_use]
    pub fn child(
        self: &Arc<Self>,
        name: impl Into<Arc<str>>,
        kind: MemoryPoolKind,
        limit_bytes: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            name: name.into(),
            kind,
            limit_bytes: AtomicU64::new(limit_bytes),
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            parent: Some(self.clone()),
            manager: self.manager.clone(),
            _backing: None,
        })
    }

    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> MemoryPoolKind {
        self.kind
    }

    #[must_use]
    pub fn limit_bytes(&self) -> u64 {
        self.limit_bytes.load(Ordering::Acquire)
    }

    /// Changes the budget used for future reservations.
    ///
    /// Lowering the budget below current usage is allowed. Existing allocations remain
    /// accounted, while new reservations wait until usage falls below the new budget.
    pub fn set_limit_bytes(&self, limit_bytes: u64) {
        let limit_bytes = self
            ._backing
            .as_ref()
            .map_or(limit_bytes, |permit| limit_bytes.min(permit.bytes()));
        self.limit_bytes.store(limit_bytes, Ordering::Release);
        self.manager.notify.notify_waiters();
    }

    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn released_bytes(&self) -> u64 {
        self.manager.released_bytes()
    }

    pub(crate) fn release_failures(&self) -> u64 {
        self.manager.release_failures()
    }

    #[must_use]
    pub fn parent(&self) -> Option<Arc<Self>> {
        self.parent.clone()
    }

    pub fn try_reserve(self: &Arc<Self>, bytes: u64) -> Result<Option<MemoryPermit>, MemoryError> {
        if bytes == 0 {
            return Ok(Some(MemoryPermit::new(self.clone(), 0)));
        }
        self.validate_request(bytes)?;
        let _guard = self.manager.accounting_lock.lock().unwrap();
        let lineage = self.lineage();

        for pool in &lineage {
            let Some(next) = pool.used_bytes().checked_add(bytes) else {
                return Err(MemoryError::AccountingOverflow);
            };
            if next > pool.limit_bytes() {
                return Ok(None);
            }
        }
        for pool in lineage {
            let next = pool.used_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
            pool.peak_bytes.fetch_max(next, Ordering::AcqRel);
        }
        Ok(Some(MemoryPermit::new(self.clone(), bytes)))
    }

    pub async fn reserve(self: &Arc<Self>, bytes: u64) -> Result<MemoryPermit, MemoryError> {
        self.validate_request(bytes)?;
        loop {
            // Register before checking to avoid missing a release between the check and await.
            let mut notified = Box::pin(self.manager.notify.notified());
            notified.as_mut().enable();
            if let Some(permit) = self.try_reserve(bytes)? {
                return Ok(permit);
            }
            let shortfall = self.reservation_shortfall(bytes)?;
            if shortfall > 0 {
                self.manager.request_release(shortfall);
            }
            if let Some(permit) = self.try_reserve(bytes)? {
                return Ok(permit);
            }
            notified.await;
        }
    }

    #[must_use]
    pub fn register_release_target(
        self: &Arc<Self>,
        name: impl Into<Arc<str>>,
    ) -> MemoryReleaseTarget {
        MemoryReleaseTarget::register(&self.manager, name)
    }

    fn validate_request(&self, bytes: u64) -> Result<(), MemoryError> {
        for pool in self.lineage() {
            let limit = pool.limit_bytes();
            if bytes > limit {
                return Err(MemoryError::RequestExceedsPoolLimit {
                    requested: bytes,
                    pool: pool.name().to_owned(),
                    limit,
                });
            }
        }
        Ok(())
    }

    fn reservation_shortfall(&self, bytes: u64) -> Result<u64, MemoryError> {
        let _guard = self.manager.accounting_lock.lock().unwrap();
        let mut shortfall = 0;
        for pool in self.lineage() {
            let limit = pool.limit_bytes();
            if bytes > limit {
                return Err(MemoryError::RequestExceedsPoolLimit {
                    requested: bytes,
                    pool: pool.name().to_owned(),
                    limit,
                });
            }
            let required = pool
                .used_bytes()
                .checked_add(bytes)
                .ok_or(MemoryError::AccountingOverflow)?;
            shortfall = shortfall.max(required.saturating_sub(limit));
        }
        Ok(shortfall)
    }

    fn lineage(&self) -> Vec<&Self> {
        let mut lineage = Vec::new();
        let mut current = Some(self);
        while let Some(pool) = current {
            lineage.push(pool);
            current = pool.parent.as_deref();
        }
        lineage
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        {
            let _guard = self.manager.accounting_lock.lock().unwrap();
            for pool in self.lineage() {
                let previous = pool.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
                debug_assert!(previous >= bytes, "memory pool accounting underflow");
            }
        }
        self.manager.notify.notify_waiters();
    }
}

/// Releases its accounted bytes from the complete pool lineage when dropped.
#[derive(Debug)]
pub struct MemoryPermit {
    pool: Arc<MemoryPool>,
    bytes: u64,
}

impl MemoryPermit {
    /// Converts an already charged reservation into an isolated allocation budget. Dropping
    /// a suballocation returns capacity to this budget, without releasing it to competitors.
    pub fn into_pool(self, name: impl Into<Arc<str>>) -> Arc<MemoryPool> {
        let mut pool = MemoryPool::new_root(
            Arc::new(ManagerState::new()),
            name,
            MemoryPoolKind::Operator,
            self.bytes,
        );
        let inner = Arc::get_mut(&mut pool).expect("new pool has a unique owner");
        inner._backing = Some(self);
        pool
    }

    fn new(pool: Arc<MemoryPool>, bytes: u64) -> Self {
        Self { pool, bytes }
    }

    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    #[must_use]
    pub fn pool(&self) -> &Arc<MemoryPool> {
        &self.pool
    }

    /// Reduces this reservation and immediately returns the difference to the pool.
    pub fn shrink_to(&mut self, bytes: u64) {
        assert!(
            bytes <= self.bytes,
            "cannot grow a memory permit with shrink_to"
        );
        self.pool.release(self.bytes - bytes);
        self.bytes = bytes;
    }

    /// Combines reservations from the same pool without releasing or reacquiring capacity.
    ///
    /// # Panics
    /// Panics if the reservations belong to different pools.
    pub fn merge(&mut self, mut other: Self) {
        assert!(
            Arc::ptr_eq(&self.pool, &other.pool),
            "cannot merge reservations from different pools"
        );
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .expect("accounted reservations cannot overflow");
        other.bytes = 0;
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ManagerState, MemoryReleaseTarget};
    use crate::{MemoryError, MemoryManager, MemoryPoolKind};

    #[test]
    fn sibling_reservations_share_ancestor_capacity_atomically() {
        let manager = MemoryManager::new(1024);
        let pipeline = manager.create_pipeline_pool("pipeline", 768);
        let first = pipeline.child("first", MemoryPoolKind::Operator, 768);
        let second = pipeline.child("second", MemoryPoolKind::Operator, 512);
        let mut held = first.try_reserve(512).unwrap().unwrap();

        assert_eq!(pipeline.used_bytes(), 512);
        assert_eq!(manager.used_bytes(), 512);
        assert!(second.try_reserve(257).unwrap().is_none());
        assert_eq!(second.used_bytes(), 0);
        assert_eq!(second.peak_bytes(), 0);
        assert_eq!(pipeline.used_bytes(), 512);
        assert!(matches!(
            second.try_reserve(513),
            Err(MemoryError::RequestExceedsPoolLimit { limit: 512, .. })
        ));

        held.shrink_to(256);
        let other = second.try_reserve(512).unwrap().unwrap();
        assert_eq!(pipeline.used_bytes(), 768);
        assert_eq!(manager.used_bytes(), 768);
        drop((held, other));
        assert_eq!(pipeline.used_bytes(), 0);
        assert_eq!(manager.used_bytes(), 0);
        assert_eq!(pipeline.peak_bytes(), 768);
    }

    #[tokio::test]
    async fn cancelling_a_waiter_does_not_charge_memory() {
        let manager = MemoryManager::new(1024);
        let pool = manager.worker_pool();
        let held = pool.reserve(1024).await.unwrap();
        let mut pending = Box::pin(pool.reserve(512));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
        assert_eq!(manager.used_bytes(), 1024);
        drop(held);
        assert_eq!(manager.used_bytes(), 0);
        assert_eq!(pool.try_reserve(1024).unwrap().unwrap().bytes(), 1024);
    }

    #[tokio::test]
    async fn pressure_on_one_pipeline_can_reclaim_another_pipeline() {
        let manager = MemoryManager::new(1024);
        let first = manager.create_pipeline_pool("first", 1024);
        let second = manager.create_pipeline_pool("second", 1024);
        let held = first.reserve(1024).await.unwrap();
        let mut target = first.register_release_target("idle-sort");
        target.set_reclaim_bytes(1024);
        let mut pending = Box::pin(second.reserve(512));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let request = target
            .try_recv()
            .expect("request sent to the other pipeline");
        assert_eq!(request.target_bytes(), 512);
        assert_eq!(second.used_bytes(), 0);
        assert_eq!(target.reclaim_bytes(), 0);
        drop(held);
        request.complete(1024);
        let granted = pending.await.unwrap();
        assert_eq!(first.used_bytes(), 0);
        assert_eq!(second.used_bytes(), 512);
        assert_eq!(manager.released_bytes(), 1024);
        drop(granted);
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn dropped_targets_unregister_without_memory_pressure() {
        let manager = Arc::new(ManagerState::new());
        let live = MemoryReleaseTarget::register(&manager, "live");
        for _ in 0..10_000 {
            let target = MemoryReleaseTarget::register(&manager, "temporary");
            assert_eq!(manager.release_targets.lock().unwrap().len(), 2);
            drop(target);
            let registered = manager.release_targets.lock().unwrap();
            assert_eq!(registered.len(), 1);
            assert!(registered.contains_key(&live.id()));
        }
        drop(live);
        assert!(manager.release_targets.lock().unwrap().is_empty());
        assert_eq!(manager.release_failures(), 0);
    }

    #[tokio::test]
    async fn unregister_during_selection_does_not_keep_an_endpoint_registered() {
        let manager = Arc::new(ManagerState::new());
        let target = MemoryReleaseTarget::register(&manager, "selected");
        target.set_reclaim_bytes(64);
        // Model request_release's temporary strong snapshot outliving the owner.
        let selected = manager.release_targets.lock().unwrap()[&target.id()]
            .upgrade()
            .unwrap();
        let weak = Arc::downgrade(&selected);
        drop(target);
        assert!(manager.release_targets.lock().unwrap().is_empty());
        assert_eq!(selected.reclaim_bytes(), 0);
        // A selector may already have claimed bytes before the owner dropped.
        let response = selected.request(64, manager.notify.clone(), Arc::downgrade(&manager));
        drop(selected);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            response.await.unwrap(),
            Err(crate::MemoryReleaseError::TargetUnavailable)
        );
        assert_eq!(manager.release_failures(), 1);
    }

    #[tokio::test]
    async fn unregister_keeps_live_targets_available_for_recovery() {
        let manager = MemoryManager::new(1024);
        let pool = manager.worker_pool();
        let held = pool.reserve(1024).await.unwrap();
        let mut live = pool.register_release_target("live");
        live.set_reclaim_bytes(1024);
        drop(pool.register_release_target("temporary"));
        let mut pending = Box::pin(pool.reserve(512));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let request = live
            .try_recv()
            .expect("live endpoint receives recovery request");
        assert_eq!(request.target_bytes(), 512);
        drop(held);
        request.complete(1024);
        let granted = pending.await.unwrap();
        assert_eq!(granted.bytes(), 512);
        drop(granted);
        drop(live);
        assert_eq!(manager.used_bytes(), 0);
        assert!(pool.manager.release_targets.lock().unwrap().is_empty());
    }

    #[test]
    fn merging_permits_preserves_accounting() {
        let manager = MemoryManager::new(1024);
        let pool = manager.worker_pool();
        let mut first = pool.try_reserve(256).unwrap().unwrap();
        first.merge(pool.try_reserve(512).unwrap().unwrap());
        assert_eq!(first.bytes(), 768);
        assert_eq!(manager.used_bytes(), 768);
        assert_eq!(pool.peak_bytes(), 768);
        drop(first);
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn reserved_pool_keeps_parent_capacity_until_last_suballocation_drops() {
        let manager = MemoryManager::new(1024);
        let permit = manager.worker_pool().try_reserve(768).unwrap().unwrap();
        let pool = permit.into_pool("merge");
        let allocation = pool.try_reserve(512).unwrap().unwrap();
        assert_eq!(manager.used_bytes(), 768);
        assert!(manager.worker_pool().try_reserve(257).unwrap().is_none());
        pool.set_limit_bytes(2048);
        assert_eq!(pool.limit_bytes(), 768);
        drop(pool);
        assert_eq!(manager.used_bytes(), 768);
        drop(allocation);
        assert_eq!(manager.used_bytes(), 0);
    }
}
