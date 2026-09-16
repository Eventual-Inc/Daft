use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{Notify, oneshot};

use crate::pool::ManagerState;

static NEXT_RELEASE_TARGET_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryReleaseOutcome {
    pub released_bytes: u64,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum MemoryReleaseError {
    #[error("memory release target is no longer available")]
    TargetUnavailable,
    #[error("memory release failed: {0}")]
    Failed(String),
}

#[derive(Debug)]
pub struct MemoryReleaseRequest {
    target_bytes: u64,
    result_tx: Option<oneshot::Sender<Result<MemoryReleaseOutcome, MemoryReleaseError>>>,
    memory_available: Arc<Notify>,
    manager: Weak<ManagerState>,
    completed: bool,
}

impl MemoryReleaseRequest {
    #[must_use]
    pub fn target_bytes(&self) -> u64 {
        self.target_bytes
    }

    pub fn complete(mut self, released_bytes: u64) {
        if let Some(manager) = self.manager.upgrade() {
            manager.record_release(released_bytes);
        }
        if let Some(result_tx) = self.result_tx.take() {
            let _ = result_tx.send(Ok(MemoryReleaseOutcome { released_bytes }));
        }
        self.completed = true;
        self.memory_available.notify_waiters();
    }

    pub fn fail(mut self, message: impl Into<String>) {
        if let Some(manager) = self.manager.upgrade() {
            manager.record_release_failure();
        }
        if let Some(result_tx) = self.result_tx.take() {
            let _ = result_tx.send(Err(MemoryReleaseError::Failed(message.into())));
        }
        self.completed = true;
        self.memory_available.notify_waiters();
    }
}

#[derive(Debug)]
pub(crate) struct ReleaseTargetInner {
    id: u64,
    name: Arc<str>,
    reclaim_bytes: AtomicU64,
    requests: Mutex<VecDeque<MemoryReleaseRequest>>,
    notify: Notify,
    manager: Weak<ManagerState>,
}

impl ReleaseTargetInner {
    pub(crate) fn new(name: impl Into<Arc<str>>, manager: Weak<ManagerState>) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_RELEASE_TARGET_ID.fetch_add(1, Ordering::Relaxed),
            name: name.into(),
            reclaim_bytes: AtomicU64::new(0),
            requests: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            manager,
        })
    }

    pub(crate) fn reclaim_bytes(&self) -> u64 {
        self.reclaim_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn claim_reclaim_bytes(&self) -> u64 {
        self.reclaim_bytes.swap(0, Ordering::AcqRel)
    }

    pub(crate) fn request(
        &self,
        target_bytes: u64,
        memory_available: Arc<Notify>,
        manager: Weak<ManagerState>,
    ) -> oneshot::Receiver<Result<MemoryReleaseOutcome, MemoryReleaseError>> {
        let (result_tx, result_rx) = oneshot::channel();
        self.requests
            .lock()
            .unwrap()
            .push_back(MemoryReleaseRequest {
                target_bytes,
                result_tx: Some(result_tx),
                memory_available,
                manager,
                completed: false,
            });
        self.notify.notify_one();
        result_rx
    }
}

impl Drop for MemoryReleaseRequest {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(manager) = self.manager.upgrade() {
            manager.record_release_failure();
        }
        if let Some(result_tx) = self.result_tx.take() {
            let _ = result_tx.send(Err(MemoryReleaseError::TargetUnavailable));
        }
        self.memory_available.notify_waiters();
    }
}

impl Drop for ReleaseTargetInner {
    fn drop(&mut self) {
        self.requests.get_mut().unwrap().clear();
    }
}

/// A release endpoint owned and polled by the execution state that can free memory.
#[derive(Debug)]
pub struct MemoryReleaseTarget {
    inner: Arc<ReleaseTargetInner>,
}

impl MemoryReleaseTarget {
    pub(crate) fn register(manager: &Arc<ManagerState>, name: impl Into<Arc<str>>) -> Self {
        let inner = ReleaseTargetInner::new(name, Arc::downgrade(manager));
        manager.register_release_target(inner.id, Arc::downgrade(&inner));
        Self { inner }
    }

    #[must_use]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn set_reclaim_bytes(&self, bytes: u64) {
        let previous = self.inner.reclaim_bytes.swap(bytes, Ordering::AcqRel);
        if previous == 0
            && bytes > 0
            && let Some(manager) = self.inner.manager.upgrade()
        {
            manager.notify_waiters();
        }
    }

    #[must_use]
    pub fn reclaim_bytes(&self) -> u64 {
        self.inner.reclaim_bytes()
    }

    pub fn withdraw(&self) {
        self.set_reclaim_bytes(0);
    }

    pub async fn recv(&mut self) -> MemoryReleaseRequest {
        loop {
            let inner = self.inner.clone();
            let notified = inner.notify.notified();
            if let Some(request) = self.try_recv() {
                return request;
            }
            notified.await;
        }
    }

    pub fn try_recv(&mut self) -> Option<MemoryReleaseRequest> {
        self.inner.requests.lock().unwrap().pop_front()
    }
}

impl Drop for MemoryReleaseTarget {
    fn drop(&mut self) {
        // Unregister the endpoint when its owner leaves, even if an in-flight
        // selection still holds a temporary Arc to the inner state. Do not wait
        // for a future memory-pressure scan to release the registry's weak ref.
        self.withdraw();
        if let Some(manager) = self.inner.manager.upgrade() {
            manager.unregister_release_target(self.inner.id);
        }
    }
}

pub(crate) type WeakReleaseTarget = Weak<ReleaseTargetInner>;
