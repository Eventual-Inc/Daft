use std::sync::Arc;

use crate::{MemoryError, MemoryPool, MemoryPoolKind, pool::ManagerState};

/// Owns the worker budget and creates accounting scopes for cached pipelines.
#[derive(Debug)]
pub struct MemoryManager {
    worker_pool: Arc<MemoryPool>,
    spill_pool: Arc<MemoryPool>,
}

impl MemoryManager {
    #[must_use]
    pub fn new(limit_bytes: u64) -> Self {
        Self::with_spill_reserve(limit_bytes, 0).expect("zero spill reserve is valid")
    }

    /// Splits the worker budget into execution capacity and shared spill workspace.
    /// Reserved capacity is not counted as used until an operation acquires a permit.
    /// Spill waiters have a separate notification/release domain: they must wait for
    /// other spill operations, not ask ordinary operators to initiate more spilling.
    pub fn with_spill_reserve(limit_bytes: u64, spill_bytes: u64) -> Result<Self, MemoryError> {
        if spill_bytes > 0 && spill_bytes >= limit_bytes {
            return Err(MemoryError::InvalidSpillReserve {
                reserved: spill_bytes,
                limit: limit_bytes,
            });
        }
        let worker_pool = MemoryPool::new_root(
            Arc::new(ManagerState::new()),
            "cpu-worker",
            MemoryPoolKind::Worker,
            limit_bytes - spill_bytes,
        );
        let spill_pool = MemoryPool::new_root(
            Arc::new(ManagerState::new()),
            "cpu-spill",
            MemoryPoolKind::Spill,
            spill_bytes,
        );
        Ok(Self {
            worker_pool,
            spill_pool,
        })
    }

    #[must_use]
    pub fn worker_pool(&self) -> Arc<MemoryPool> {
        self.worker_pool.clone()
    }

    #[must_use]
    pub fn spill_pool(&self) -> Arc<MemoryPool> {
        self.spill_pool.clone()
    }

    #[must_use]
    pub fn create_pipeline_pool(
        &self,
        name: impl Into<Arc<str>>,
        limit_bytes: u64,
    ) -> Arc<MemoryPool> {
        self.worker_pool.child(
            name,
            MemoryPoolKind::Pipeline,
            limit_bytes.min(self.worker_pool.limit_bytes()),
        )
    }

    /// Reserves worker memory for execution admission that has not yet been assigned
    /// to a more specific execution scope.
    pub async fn reserve(&self, bytes: u64) -> Result<crate::MemoryPermit, crate::MemoryError> {
        self.worker_pool.reserve(bytes).await
    }

    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.worker_pool
            .limit_bytes()
            .saturating_add(self.spill_pool.limit_bytes())
    }

    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.worker_pool
            .used_bytes()
            .saturating_add(self.spill_pool.used_bytes())
    }

    #[must_use]
    pub fn released_bytes(&self) -> u64 {
        self.worker_pool.released_bytes()
    }

    #[must_use]
    pub fn release_failures(&self) -> u64 {
        self.worker_pool.release_failures()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execution_cannot_consume_spill_capacity() {
        let manager = MemoryManager::with_spill_reserve(1024, 256).unwrap();
        let pipeline = manager.create_pipeline_pool("pipeline", 1024);
        assert_eq!(pipeline.limit_bytes(), 768);
        assert_eq!(manager.used_bytes(), 0);
        assert_eq!(manager.total_bytes(), 1024);
        let execution = pipeline.reserve(768).await.unwrap();
        let workspace = manager.spill_pool().reserve(256).await.unwrap();
        assert_eq!(manager.used_bytes(), 1024);
        assert!(pipeline.try_reserve(1).unwrap().is_none());
        // A request which can never fit must fail, not wait for reserved capacity.
        assert!(matches!(
            manager.reserve(769).await,
            Err(MemoryError::RequestExceedsPoolLimit { limit: 768, .. })
        ));
        drop(workspace);
        assert_eq!(manager.used_bytes(), 768);
        assert!(pipeline.try_reserve(1).unwrap().is_none());
        drop(execution);
        assert_eq!(manager.used_bytes(), 0);
    }

    #[tokio::test]
    async fn spill_waiters_share_capacity_and_wake_on_release() {
        let manager = MemoryManager::with_spill_reserve(1024, 256).unwrap();
        let spill_pool = manager.spill_pool();
        let first = spill_pool.reserve(256).await.unwrap();
        let mut second = Box::pin(spill_pool.reserve(128));
        std::future::poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(first);
        let second = second.await.unwrap();
        assert_eq!(manager.used_bytes(), 128);
        drop(second);
        assert_eq!(manager.used_bytes(), 0);
    }

    #[test]
    fn invalid_split_is_rejected() {
        assert!(MemoryManager::with_spill_reserve(1024, 1024).is_err());
        assert!(MemoryManager::with_spill_reserve(1024, 1025).is_err());
        assert_eq!(MemoryManager::new(1024).worker_pool().limit_bytes(), 1024);
    }
}
