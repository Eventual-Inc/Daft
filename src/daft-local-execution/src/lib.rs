#![feature(associated_type_defaults)]

mod batch_manager;
mod buffer;
mod channel;
mod checkpoint_terminus;
mod concat;
mod dynamic_batching;
mod input_sender;
mod intermediate_ops;
mod join;
mod memory_size;
mod pipeline;
mod resource_manager;
mod run;
mod runtime_stats;
mod sinks;
mod sources;
pub mod spilling;
mod streaming_sink;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use arc_swap::ArcSwap;
use common_error::{DaftError, DaftResult};
use common_runtime::{JoinSet, RuntimeRef, RuntimeTask};
use console::style;
use daft_memory::{MemoryPermit, MemoryPool};
use resource_manager::PipelineMemoryContext;
pub use run::ExecutionEngineResult;

/// Helpers for distributed execution tests.
///
/// Used by `LocalSwordfishWorker` in `daft-distributed` to exercise real
/// local execution without Ray.
pub mod testing {
    pub use super::run::NativeExecutor;
}
use runtime_stats::RuntimeStatsManagerHandle;
use snafu::{ResultExt, Snafu, futures::TryFutureExt};
use spilling::SpillManager;
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

impl<T: Send + Unpin + 'static> Future for OperatorOutput<T> {
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

impl<T: Send + 'static> From<T> for OperatorOutput<T> {
    fn from(value: T) -> Self {
        Self::Ready(Some(value))
    }
}

impl<T: Send + 'static> From<RuntimeTask<T>> for OperatorOutput<T> {
    fn from(task: RuntimeTask<T>) -> Self {
        Self::Pending(task)
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

pub(crate) struct ExecutionRuntimeContext {
    worker_set: JoinSet<Result<()>>,
    memory_context: Arc<PipelineMemoryContext>,
    pub(crate) spill_manager: SpillManager,
    stats_manager: RuntimeStatsManagerHandle,
}

impl ExecutionRuntimeContext {
    #[must_use]
    pub fn new(
        memory_pool: Arc<MemoryPool>,
        spill_manager: SpillManager,
        stats_manager: RuntimeStatsManagerHandle,
    ) -> Self {
        Self {
            worker_set: JoinSet::new(),
            memory_context: Arc::new(PipelineMemoryContext::new(memory_pool)),
            spill_manager,
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

    pub async fn join_next(&mut self) -> Option<DaftResult<()>> {
        match self.worker_set.join_next().await {
            Some(Ok(Ok(()))) => Some(Ok(())),
            Some(Ok(Err(e))) => Some(Err(e.into())),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    pub async fn shutdown(&mut self) -> DaftResult<()> {
        self.worker_set.abort_all();
        while let Some(result) = self.worker_set.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(e) => {
                    // Only suppress errors that are JoinError caused by cancellation
                    if let DaftError::JoinError(ref join_err) = e
                        && join_err.is_cancelled()
                    {
                        continue;
                    }

                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn memory_pool(&self) -> Arc<MemoryPool> {
        self.memory_context.pipeline_pool()
    }

    #[must_use]
    pub(crate) fn memory_context(&self) -> Arc<PipelineMemoryContext> {
        self.memory_context.clone()
    }

    #[must_use]
    pub(crate) fn stats_manager(&self) -> RuntimeStatsManagerHandle {
        self.stats_manager.clone()
    }

    #[must_use]
    pub(crate) fn spill_manager(&self) -> SpillManager {
        self.spill_manager.clone()
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionTaskSpawner {
    runtime_ref: RuntimeRef,
    memory_pool: Arc<MemoryPool>,
    pub(crate) spill_manager: SpillManager,
    outer_span: tracing::Span,
}

impl ExecutionTaskSpawner {
    pub fn new(
        runtime_ref: RuntimeRef,
        memory_pool: Arc<MemoryPool>,
        spill_manager: SpillManager,
        span: tracing::Span,
    ) -> Self {
        Self {
            runtime_ref,
            memory_pool,
            spill_manager,
            outer_span: span,
        }
    }

    #[must_use]
    pub fn with_memory_pool(&self, memory_pool: Arc<MemoryPool>) -> Self {
        Self {
            runtime_ref: self.runtime_ref.clone(),
            memory_pool,
            spill_manager: self.spill_manager.clone(),
            outer_span: self.outer_span.clone(),
        }
    }

    /// Reserves memory that may outlive a single spawned task.
    ///
    /// The caller can store the returned permit alongside operator state. The memory remains
    /// accounted until the permit is dropped.
    pub async fn reserve_memory(&self, bytes: u64) -> DaftResult<MemoryPermit> {
        self.memory_pool
            .reserve(bytes)
            .await
            .map_err(|error| DaftError::ComputeError(error.to_string()))
    }

    /// Attempts to reserve memory without waiting for another operator to release memory.
    pub fn try_reserve_memory(&self, bytes: u64) -> DaftResult<Option<MemoryPermit>> {
        self.memory_pool
            .try_reserve(bytes)
            .map_err(|error| DaftError::ComputeError(error.to_string()))
    }

    #[must_use]
    pub fn memory_limit_bytes(&self) -> u64 {
        self.memory_pool.limit_bytes()
    }

    pub fn register_release_target(&self, name: &str) -> daft_memory::MemoryReleaseTarget {
        self.memory_pool.register_release_target(name)
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
        let outer_span = self.outer_span.clone();
        let task_spawner = self.clone();
        self.runtime_ref.spawn(async move {
            let _permit = task_spawner.reserve_memory(memory_request).await?;
            future.instrument(span).instrument(outer_span).await
        })
    }

    pub fn spawn<F, O>(&self, future: F, inner_span: tracing::Span) -> RuntimeTask<DaftResult<O>>
    where
        F: Future<Output = DaftResult<O>> + Send + 'static,
        O: Send + 'static,
    {
        self.runtime_ref.spawn(
            future
                .instrument(inner_span)
                .instrument(self.outer_span.clone()),
        )
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
