mod channel;
mod intermediate_ops;
mod pipeline;
mod run;
mod sinks;
mod sources;

use std::env;

use common_error::DaftError;
pub use run::NativeExecutor;
use snafu::Snafu;

use lazy_static::lazy_static;
lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

fn get_morsel_size() -> usize {
    env::var("DAFT_MORSEL_SIZE")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .expect("OUTPUT_THRESHOLD must be a number")
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
