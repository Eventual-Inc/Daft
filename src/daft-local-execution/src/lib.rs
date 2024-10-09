#![feature(let_chains)]
mod channel;
mod intermediate_ops;
mod pipeline;
mod run;
mod runtime_stats;
mod sinks;
mod sources;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use lazy_static::lazy_static;
pub use run::NativeExecutor;
use snafu::{futures::TryFutureExt, Snafu};
lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

static COMPUTE_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();

pub type RuntimeRef = Arc<Runtime>;

pub struct Runtime {
    runtime: tokio::runtime::Runtime,
}

impl Runtime {
    fn new(runtime: tokio::runtime::Runtime) -> RuntimeRef {
        Arc::new(Self { runtime })
    }

    pub async fn await_on_compute_pool<F>(&self, future: F) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _join_handle = self.spawn(async move {
            let task_output = AssertUnwindSafe(future).catch_unwind().await.map_err(|e| {
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
            });

            if tx.send(task_output).is_err() {
                log::warn!("Spawned task output ignored: receiver dropped");
            }
        });
        rx.await.expect("Compute pool receiver dropped")
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}

fn init_runtime(num_threads: usize) -> Arc<Runtime> {
    std::thread::spawn(move || {
        Runtime::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_threads)
                .enable_all()
                .thread_name_fn(|| {
                    static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("Executor-Worker-{}", id)
                })
                .build()
                .unwrap(),
        )
    })
    .join()
    .unwrap()
}

pub fn get_compute_runtime() -> DaftResult<RuntimeRef> {
    let runtime = COMPUTE_RUNTIME
        .get_or_init(|| init_runtime(*NUM_CPUS))
        .clone();
    Ok(runtime)
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
use tokio::task::JoinHandle;

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
