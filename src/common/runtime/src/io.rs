use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use tokio::task::JoinHandle;

pub(crate) static THREADED_IO_RUNTIME: OnceLock<IORuntimeRef> = OnceLock::new();
pub(crate) static SINGLE_THREADED_IO_RUNTIME: OnceLock<IORuntimeRef> = OnceLock::new();

pub type IORuntimeRef = Arc<IORuntime>;

pub struct IORuntime {
    runtime: tokio::runtime::Runtime,
}

impl IORuntime {
    pub(crate) fn new(runtime: tokio::runtime::Runtime) -> IORuntimeRef {
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
                "Caught panic when spawning blocking task in IO pool {s})"
            ))
        })
    }

    /// Similar to tokio's Runtime::block_on but requires static lifetime + Send
    /// You should use this when you are spawning IO tasks from an Expression Evaluator or in the Executor
    pub fn block_on<F>(&self, future: F) -> DaftResult<F::Output>
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
        rx.recv().expect("Spawned task transmitter dropped")
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

    /// Blocks current thread to compute future. Can not be called in tokio runtime context
    ///
    pub fn block_on_current_thread<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}
