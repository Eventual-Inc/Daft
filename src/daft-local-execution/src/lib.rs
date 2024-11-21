#![feature(let_chains)]
mod buffer;
mod channel;
mod dispatcher;
mod intermediate_ops;
mod pipeline;
mod run;
mod runtime_stats;
mod sinks;
mod sources;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_runtime::RuntimeTask;
use lazy_static::lazy_static;
pub use run::{run_local, NativeExecutor};
use snafu::{futures::TryFutureExt, ResultExt, Snafu};

lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

/// The `OperatorOutput` enum represents the output of an operator.
/// It can be either `Ready` or `Pending`.
/// If the output is `Ready`, the value is immediately available.
/// If the output is `Pending`, the value is not yet available and a `RuntimeTask` is returned.
#[pin_project::pin_project(project = OperatorOutputProj)]
pub(crate) enum OperatorOutput<T> {
    Ready(Option<T>),
    Pending(#[pin] RuntimeTask<T>),
}

impl<T: Send + Sync + Unpin + 'static> Future for OperatorOutput<T> {
    type Output = DaftResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            OperatorOutputProj::Ready(value) => {
                let value = value.take().unwrap();
                Poll::Ready(Ok(value))
            }
            OperatorOutputProj::Pending(task) => task.poll(cx),
        }
    }
}

impl<T: Send + Sync + 'static> From<T> for OperatorOutput<T> {
    fn from(value: T) -> Self {
        Self::Ready(Some(value))
    }
}

impl<T: Send + Sync + 'static> From<RuntimeTask<T>> for OperatorOutput<T> {
    fn from(task: RuntimeTask<T>) -> Self {
        Self::Pending(task)
    }
}

pub(crate) struct TaskSet<T> {
    inner: tokio::task::JoinSet<T>,
}

impl<T: 'static> TaskSet<T> {
    fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    fn spawn<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = T> + 'static,
    {
        self.inner.spawn_local(future);
    }

    async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        self.inner.join_next().await
    }

    async fn shutdown(&mut self) {
        self.inner.shutdown().await;
    }
}

#[pin_project::pin_project]
struct SpawnedTask<T>(#[pin] tokio::task::JoinHandle<T>);
impl<T> Future for SpawnedTask<T> {
    type Output = crate::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().0.poll(cx).map(|r| r.context(JoinSnafu))
    }
}

struct RuntimeHandle(tokio::runtime::Handle);
impl RuntimeHandle {
    fn spawn<F>(&self, future: F) -> SpawnedTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let join_handle = self.0.spawn(future);
        SpawnedTask(join_handle)
    }
}

pub struct ExecutionRuntimeContext {
    worker_set: TaskSet<crate::Result<()>>,
    default_morsel_size: usize,
}

impl ExecutionRuntimeContext {
    #[must_use]
    pub fn new(default_morsel_size: usize) -> Self {
        Self {
            worker_set: TaskSet::new(),
            default_morsel_size,
        }
    }
    pub fn spawn(
        &mut self,
        task: impl std::future::Future<Output = DaftResult<()>> + 'static,
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

    pub(crate) fn handle(&self) -> RuntimeHandle {
        RuntimeHandle(tokio::runtime::Handle::current())
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
