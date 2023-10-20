#![feature(async_closure)]
#![feature(let_chains)]
use common_error::DaftError;
use snafu::Snafu;

mod compression;
pub mod metadata;
#[cfg(feature = "python")]
pub mod python;
pub mod read;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    IOError { source: daft_io::Error },
    #[snafu(display("{source}"))]
    CSVError { source: csv_async::Error },
    #[snafu(display("{source}"))]
    ArrowError { source: arrow2::error::Error },
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
        match err {
            Error::IOError { source } => source.into(),
            _ => DaftError::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for Error {
    fn from(err: daft_io::Error) -> Self {
        Error::IOError { source: err }
    }
}
