mod channel;
mod intermediate_ops;
mod pipeline;
mod run;
mod sinks;
mod sources;

use common_error::{DaftError, DaftResult};
pub use run::NativeExecutor;
use snafu::{ResultExt, Snafu};

use lazy_static::lazy_static;
lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

// A TaskSet is a collection of tasks that can be spawned and joined on.
pub struct TaskSet<T: Send + 'static> {
    join_set: tokio::task::JoinSet<DaftResult<T>>,
}

impl Default for TaskSet<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> TaskSet<T> {
    pub fn new() -> Self {
        Self {
            join_set: tokio::task::JoinSet::new(),
        }
    }

    pub fn spawn<F>(&mut self, f: F)
    where
        F: std::future::Future<Output = DaftResult<T>> + Send + 'static,
    {
        self.join_set.spawn(f);
    }

    pub async fn join_next(&mut self) -> Option<Result<DaftResult<T>, tokio::task::JoinError>> {
        self.join_set.join_next().await
    }

    pub async fn join_all(&mut self) -> DaftResult<()> {
        while let Some(result) = self.join_set.join_next().await {
            result.context(JoinSnafu {})??;
        }
        Ok(())
    }
}

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
