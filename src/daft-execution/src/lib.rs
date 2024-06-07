#![feature(impl_trait_in_assoc_type)]
#![feature(let_chains)]
#![feature(assert_matches)]
// TODO(Clark): Remove this once stage planner, partial metadata, etc. are implemented.
#![allow(dead_code)]
#![allow(unused)]

mod ops;
mod partition;

use common_error::DaftError;
use snafu::Snafu;

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
    Ok(())
}
