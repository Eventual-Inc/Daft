use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use tokio::task::JoinHandle;

pub(crate) static COMPUTE_RUNTIME: OnceLock<ComputeRuntimeRef> = OnceLock::new();
pub type ComputeRuntimeRef = Arc<ComputeRuntime>;

pub struct ComputeRuntime {
    runtime: tokio::runtime::Runtime,
}

impl ComputeRuntime {
    pub(crate) fn new(runtime: tokio::runtime::Runtime) -> ComputeRuntimeRef {
        Arc::new(Self { runtime })
    }

    async fn execute_task<F>(future: F) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        AssertUnwindSafe(future).catch_unwind().await.map_err(|e| {
            let s = if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = e.downcast_ref::<&str>() {
                (*s).to_string()
            } else {
                "unknown internal error".to_string()
            };
            DaftError::ComputeError(format!(
                "Caught panic when spawning blocking task in compute pool {s})"
            ))
        })
    }

    /// Similar to block_on, but is async and can be awaited
    pub async fn await_on<F>(&self, future: F) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let _join_handle = self.spawn(async move {
            let task_output = Self::execute_task(future).await;
            if tx.send(task_output).is_err() {
                log::warn!("Spawned task output ignored: receiver dropped");
            }
        });
        rx.await.expect("Spawned task transmitter dropped")
    }

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}
