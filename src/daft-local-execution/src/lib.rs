#![feature(let_chains)]

mod buffer;
mod channel;
mod dispatcher;
mod intermediate_ops;
mod pipeline;
mod progress_bar;
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
    sync::Arc,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeRef, RuntimeTask};
use progress_bar::{OperatorProgressBar, ProgressBarColor, ProgressBarManager};
use resource_manager::MemoryManager;
pub use run::{ExecutionEngineResult, NativeExecutor};
use runtime_stats::{RuntimeStatsContext, TimedFuture};
use snafu::{futures::TryFutureExt, ResultExt, Snafu};
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

impl<T: 'static> TaskSet<T> {
    fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    fn spawn_local<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = T> + 'static,
    {
        self.inner.spawn_local(future);
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

pub(crate) struct ExecutionRuntimeContext {
    worker_set: TaskSet<crate::Result<()>>,
    default_morsel_size: usize,
    memory_manager: Arc<MemoryManager>,
    progress_bar_manager: Option<Arc<dyn ProgressBarManager>>,
    rt_stats_handler: Arc<RuntimeStatsEventHandler>,
}

impl ExecutionRuntimeContext {
    #[must_use]
    pub fn new(
        default_morsel_size: usize,
        memory_manager: Arc<MemoryManager>,
        progress_bar_manager: Option<Arc<dyn ProgressBarManager>>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
    ) -> Self {
        Self {
            worker_set: TaskSet::new(),
            default_morsel_size,
            memory_manager,
            progress_bar_manager,
            rt_stats_handler,
        }
    }
    pub fn spawn_local(
        &mut self,
        task: impl std::future::Future<Output = DaftResult<()>> + 'static,
        node_name: &str,
    ) {
        let node_name = node_name.to_string();
        self.worker_set
            .spawn_local(task.with_context(|_| PipelineExecutionSnafu { node_name }));
    }

    pub async fn join_next(&mut self) -> Option<Result<crate::Result<()>, Error>> {
        self.worker_set.join_next().await
    }

    pub async fn shutdown(&mut self) {
        self.worker_set.shutdown().await;
    }

    #[must_use]
    pub fn default_morsel_size(&self) -> usize {
        self.default_morsel_size
    }

    pub fn make_progress_bar(
        &self,
        prefix: &'static str,
        color: ProgressBarColor,
        node_id: usize,
        runtime_stats: Arc<RuntimeStatsContext>,
    ) -> Option<Arc<OperatorProgressBar>> {
        if let Some(ref pb_manager) = self.progress_bar_manager {
            let pb = pb_manager.make_new_bar(color, prefix, node_id).unwrap();
            Some(Arc::new(OperatorProgressBar::new(pb, runtime_stats)))
        } else {
            None
        }
    }

    pub(crate) fn handle(&self) -> RuntimeHandle {
        RuntimeHandle(tokio::runtime::Handle::current())
    }

    #[must_use]
    pub(crate) fn memory_manager(&self) -> Arc<MemoryManager> {
        self.memory_manager.clone()
    }

    #[must_use]
    pub(crate) fn runtime_stats_handler(&self) -> Arc<RuntimeStatsEventHandler> {
        self.rt_stats_handler.clone()
    }
}

impl Drop for ExecutionRuntimeContext {
    fn drop(&mut self) {
        if let Some(pbm) = self.progress_bar_manager.take() {
            let _ = pbm.close_all();
        }
    }
}

pub(crate) struct ExecutionTaskSpawner {
    runtime_ref: RuntimeRef,
    memory_manager: Arc<MemoryManager>,
    runtime_context: Arc<RuntimeStatsContext>,
    rt_stats_handler: Arc<RuntimeStatsEventHandler>,
    outer_span: tracing::Span,
}

impl ExecutionTaskSpawner {
    pub fn new(
        runtime_ref: RuntimeRef,
        memory_manager: Arc<MemoryManager>,
        runtime_context: Arc<RuntimeStatsContext>,
        rt_stats_handler: Arc<RuntimeStatsEventHandler>,
        span: tracing::Span,
    ) -> Self {
        Self {
            runtime_ref,
            memory_manager,
            runtime_context,
            rt_stats_handler,
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
            self.runtime_context.clone(),
            self.rt_stats_handler.clone(),
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
            self.runtime_context.clone(),
            self.rt_stats_handler.clone(),
            self.outer_span.clone(),
        );
        self.runtime_ref.spawn(timed_fut)
    }
}

#[cfg(feature = "python")]
use pyo3::prelude::*;

use crate::runtime_stats::RuntimeStatsEventHandler;

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
    use run::PyNativeExecutor;

    parent.add_class::<PyNativeExecutor>()?;
    Ok(())
}
