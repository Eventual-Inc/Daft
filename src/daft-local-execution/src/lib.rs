#![feature(let_chains)]
mod channel;
mod intermediate_ops;
mod pipeline;
mod run;
mod runtime_stats;
mod sinks;
mod sources;
use common_error::{DaftError, DaftResult};
use lazy_static::lazy_static;
pub use run::NativeExecutor;
use snafu::{futures::TryFutureExt, Snafu};
lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

pub struct ExecutionRuntimeHandle {
    worker_set: tokio::task::JoinSet<crate::Result<()>>,
    default_morsel_size: usize,
}

impl ExecutionRuntimeHandle {
    #[must_use]
    pub fn new(default_morsel_size: usize) -> Self {
        Self {
            worker_set: tokio::task::JoinSet::new(),
            default_morsel_size,
        }
    }
    pub fn spawn(
        &mut self,
        task: impl std::future::Future<Output = DaftResult<()>> + Send + 'static,
        node_name: &str,
    ) {
        let node_name = node_name.to_string();
        self.worker_set
            .spawn(task.with_context(|_| PipelineExecutionSnafu { node_name }));
    }

    pub async fn join_next(&mut self) -> Option<Result<crate::Result<()>, tokio::task::JoinError>> {
        self.worker_set.join_next().await
    }

    pub async fn shutdown(&mut self) {
        self.worker_set.shutdown().await;
    }

    #[must_use]
    pub fn default_morsel_size(&self) -> usize {
        self.default_morsel_size
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
    #[cfg(feature = "python")]
    #[snafu(display("PyIOError: {}", source))]
    PyIO { source: PyErr },
    #[snafu(display("Error creating pipeline from {}: {}", plan_name, source))]
    PipelineCreationError {
        source: DaftError,
        plan_name: String,
    },
    #[snafu(display("Error when running pipeline node {}: {}", node_name, source))]
    PipelineExecutionError {
        source: DaftError,
        node_name: String,
    },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        match err {
            Error::PipelineCreationError { source, plan_name } => {
                log::error!("Error creating pipeline from {}", plan_name);
                source
            }
            Error::PipelineExecutionError { source, node_name } => {
                log::error!("Error when running pipeline node {}", node_name);
                source
            }
            _ => Self::External(err.into()),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<NativeExecutor>()?;
    Ok(())
}
