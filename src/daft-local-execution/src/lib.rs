mod create_pipeline;
mod intermediate_ops;
mod pipeline;
mod run;
mod sinks;
mod sources;

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;
pub use run::NativeExecutor;
use snafu::Snafu;

type Sender = tokio::sync::mpsc::Sender<DaftResult<Arc<MicroPartition>>>;
type Receiver = tokio::sync::mpsc::Receiver<DaftResult<Arc<MicroPartition>>>;

pub fn create_channel() -> (Sender, Receiver) {
    tokio::sync::mpsc::channel(1)
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
