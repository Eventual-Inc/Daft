use std::sync::{Arc, OnceLock};

/// BroadcastStateBridge is a bridge to send state from one node to another.
/// e.g. from the build phase to the probe phase of a join operation.
pub(crate) type BroadcastStateBridgeRef<T> = Arc<BroadcastStateBridge<T>>;
pub(crate) struct BroadcastStateBridge<T> {
    inner: OnceLock<Arc<T>>,
    notify: tokio::sync::Notify,
}

impl<T> BroadcastStateBridge<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: OnceLock::new(),
            notify: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn set_state(&self, state: Arc<T>) {
        assert!(
            !self.inner.set(state).is_err(),
            "BroadcastStateBridge should be set only once"
        );
        self.notify.notify_waiters();
    }

    pub(crate) async fn get_state(&self) -> Arc<T> {
        loop {
            if let Some(state) = self.inner.get() {
                return state.clone();
            }
            self.notify.notified().await;
        }
    }
}
