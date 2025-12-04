mod buffer;
mod channel;
mod dispatcher;
mod dynamic_batching;
mod intermediate_ops;
mod pipeline;
mod resource_manager;
mod run;
mod runtime_stats;
mod sinks;
mod sources;
mod state_bridge;
mod streaming_sink;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use arc_swap::ArcSwap;
use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeRef, RuntimeTask};
use console::style;
use resource_manager::MemoryManager;
pub use run::{ExecutionEngineResult, NativeExecutor};
use runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle, TimedFuture};
use snafu::{ResultExt, Snafu, futures::TryFutureExt};
use tracing::Instrument;

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

impl<T: Send + 'static> TaskSet<T> {
    fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    fn spawn<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send,
    {
        self.inner.spawn(future);
    }

    async fn join_next(&mut self) -> Option<Result<T, Error>> {
        self.inner
            .join_next()
            .await
            .map(|r| r.map_err(|e| Error::JoinError { source: e }))
    }

    fn try_join_next(&mut self) -> Option<Result<T, Error>> {
        self.inner
            .try_join_next()
            .map(|r| r.map_err(|e| Error::JoinError { source: e }))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn abort_all(&mut self) {
        self.inner.abort_all();
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

pub(crate) struct ExecutionRuntimeContext {
    worker_set: TaskSet<crate::Result<()>>,
    memory_manager: Arc<MemoryManager>,
    stats_manager: RuntimeStatsManagerHandle,
}

impl ExecutionRuntimeContext {
    #[must_use]
    pub fn new(
        memory_manager: Arc<MemoryManager>,
        stats_manager: RuntimeStatsManagerHandle,
    ) -> Self {
        Self {
            worker_set: TaskSet::new(),
            memory_manager,
            stats_manager,
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

    pub async fn join_next(&mut self) -> Option<Result<crate::Result<()>, Error>> {
        self.worker_set.join_next().await
    }

    pub async fn shutdown(&mut self) -> DaftResult<()> {
        self.worker_set.abort_all();
        while let Some(result) = self.join_next().await {
            match result {
                Ok(Err(e)) | Err(e) if !matches!(&e, Error::JoinError { source } if source.is_cancelled()) =>
                {
                    return Err(e.into());
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn handle(&self) -> RuntimeHandle {
        RuntimeHandle(tokio::runtime::Handle::current())
    }

    #[must_use]
    pub(crate) fn memory_manager(&self) -> Arc<MemoryManager> {
        self.memory_manager.clone()
    }

    #[must_use]
    pub(crate) fn stats_manager(&self) -> RuntimeStatsManagerHandle {
        self.stats_manager.clone()
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionTaskSpawner {
    runtime_ref: RuntimeRef,
    memory_manager: Arc<MemoryManager>,
    runtime_stats: Arc<dyn RuntimeStats>,
    outer_span: tracing::Span,
}

impl ExecutionTaskSpawner {
    pub fn new(
        runtime_ref: RuntimeRef,
        memory_manager: Arc<MemoryManager>,
        runtime_stats: Arc<dyn RuntimeStats>,
        span: tracing::Span,
    ) -> Self {
        Self {
            runtime_ref,
            memory_manager,
            runtime_stats,
            outer_span: span,
        }
    }

    pub fn spawn_with_memory_request<F, O>(
        &self,
        memory_request: u64,
        future: F,
        span: tracing::Span,
    ) -> RuntimeTask<DaftResult<O>>
    where
        F: Future<Output = DaftResult<O>> + Send + 'static,
        O: Send + 'static,
    {
        let instrumented = future.instrument(span);
        let timed_fut = TimedFuture::new(
            instrumented,
            self.runtime_stats.clone(),
            self.outer_span.clone(),
        );
        let memory_manager = self.memory_manager.clone();
        self.runtime_ref.spawn(async move {
            let _permit = memory_manager.request_bytes(memory_request).await?;
            timed_fut.await
        })
    }

    pub fn spawn<F, O>(&self, future: F, inner_span: tracing::Span) -> RuntimeTask<DaftResult<O>>
    where
        F: Future<Output = DaftResult<O>> + Send + 'static,
        O: Send + 'static,
    {
        let instrumented = future.instrument(inner_span);
        let timed_fut = TimedFuture::new(
            instrumented,
            self.runtime_stats.clone(),
            self.outer_span.clone(),
        );
        self.runtime_ref.spawn(timed_fut)
    }
}

// ---------------------------- STDOUT / STDERR PIPING ---------------------------- //

/// Target for printing to.
trait PythonPrintTarget: Send + Sync + 'static {
    fn println(&self, message: &str);
}

/// A static entity that redirects Python sys.stdout / sys.stderr to handle Rust side effects.
/// Tracks internal tags to reduce interweaving of user prints
/// Can also register callbacks, for example for suspending the progress bar before prints.
struct StdoutHandler {
    target: ArcSwap<Option<Box<dyn PythonPrintTarget>>>,
}

impl StdoutHandler {
    pub fn new() -> Self {
        Self {
            target: ArcSwap::new(Arc::new(None)),
        }
    }

    fn set_target(&self, target: Box<dyn PythonPrintTarget>) {
        self.target.store(Arc::new(Some(target)));
    }

    fn reset_target(&self) {
        self.target.store(Arc::new(None));
    }

    fn print(&self, prefix: &str, message: &str) {
        let message = format!("{} {}", style(prefix).magenta(), message);

        if let Some(target) = self.target.load().as_ref() {
            target.println(&message);
        } else {
            println!("{message}");
        }
    }
}

static STDOUT: LazyLock<Arc<StdoutHandler>> = LazyLock::new(|| Arc::new(StdoutHandler::new()));

// -------------------------------------------------------------------------------- //

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
    #[snafu(display("ValueError: {}", message))]
    ValueError { message: String },
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
            Error::ValueError { message } => Self::ValueError(message),
            _ => Self::External(err.into()),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use run::PyNativeExecutor;

    parent.add_class::<PyNativeExecutor>()?;
    Ok(())
}
