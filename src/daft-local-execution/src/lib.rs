#![feature(let_chains)]
mod channel;
mod intermediate_ops;
mod pipeline;
mod run;
mod sinks;
mod sources;

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use lazy_static::lazy_static;
pub use run::NativeExecutor;
use snafu::Snafu;
use tokio::sync::Mutex;

lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

pub struct ExecutionRuntimeHandle {
    pub worker_set: Arc<Mutex<tokio::task::JoinSet<DaftResult<()>>>>,
}

impl Default for ExecutionRuntimeHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionRuntimeHandle {
    pub fn new() -> Self {
        Self {
            worker_set: Arc::new(Mutex::new(tokio::task::JoinSet::new())),
        }
    }
    pub async fn spawn(
        &self,
        task: impl std::future::Future<Output = DaftResult<()>> + Send + 'static,
    ) {
        let mut guard = self.worker_set.lock().await;
        guard.spawn(task);
    }

    pub async fn join_next(&self) -> Option<Result<DaftResult<()>, tokio::task::JoinError>> {
        let mut guard = self.worker_set.lock().await;
        guard.join_next().await
    }

    pub async fn shutdown(&self) {
        let mut guard = self.worker_set.lock().await;
        guard.shutdown().await;
    }
}

const DEFAULT_MORSEL_SIZE: usize = 1000;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error joining spawned task: {}", source))]
    JoinError { source: tokio::task::JoinError },
    #[snafu(display(
        "Sender of OneShot Channel Dropped before sending data over: {}",
        source
    ))]
    OneShotRecvError {
        source: tokio::sync::oneshot::error::RecvError,
    },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        DaftError::External(err.into())
    }
}

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<NativeExecutor>()?;
    Ok(())
}
