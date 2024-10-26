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

use std::sync::{Arc, OnceLock};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use daft_table::ProbeState;
use lazy_static::lazy_static;
pub use run::NativeExecutor;
use snafu::{futures::TryFutureExt, Snafu};

lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

pub(crate) type ProbeStateBridgeRef = Arc<ProbeStateBridge>;
pub(crate) struct ProbeStateBridge {
    inner: OnceLock<Arc<ProbeState>>,
    notify: tokio::sync::Notify,
}

impl ProbeStateBridge {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: OnceLock::new(),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn set_probe_state(&self, state: Arc<ProbeState>) {
        assert!(
            !self.inner.set(state).is_err(),
            "ProbeStateBridge should be set only once"
        );
        self.notify.notify_waiters();
    }

    async fn get_probe_state(&self) -> Arc<ProbeState> {
        loop {
            if let Some(state) = self.inner.get() {
                return state.clone();
            }
            self.notify.notified().await;
        }
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

pub(crate) struct ExecutionRuntimeHandle {
    worker_set: TaskSet<crate::Result<()>>,
    default_morsel_size: usize,
}

impl ExecutionRuntimeHandle {
    #[must_use]
    fn new(default_morsel_size: usize) -> Self {
        Self {
            worker_set: TaskSet::new(),
            default_morsel_size,
        }
    }
    fn spawn(
        &mut self,
        task: impl std::future::Future<Output = DaftResult<()>> + 'static,
        node_name: &str,
    ) {
        let node_name = node_name.to_string();
        self.worker_set
            .spawn(task.with_context(|_| PipelineExecutionSnafu { node_name }));
    }

    async fn join_next(&mut self) -> Option<Result<crate::Result<()>, tokio::task::JoinError>> {
        self.worker_set.join_next().await
    }

    async fn shutdown(&mut self) {
        self.worker_set.shutdown().await;
    }

    fn determine_morsel_size(&self, operator_morsel_size: Option<usize>) -> Option<usize> {
        match operator_morsel_size {
            None => None,
            Some(_)
                if self.default_morsel_size
                    != DaftExecutionConfig::default().default_morsel_size =>
            {
                Some(self.default_morsel_size)
            }
            size => size,
        }
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
