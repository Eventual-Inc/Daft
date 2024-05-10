#![feature(async_closure)]
#![feature(let_chains)]
#![feature(trait_upcasting)]
use common_error::DaftError;
use futures::stream::TryChunksError;
use snafu::Snafu;

mod decoding;
mod deserializer;
mod inference;
pub mod local;

pub mod options;
#[cfg(feature = "python")]
pub mod python;
pub mod read;
pub mod schema;

// pub use metadata::read_json_schema_bulk;
pub use options::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use read::{read_json, read_json_bulk};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    IOError { source: daft_io::Error },
    #[snafu(display("{source}"))]
    StdIOError { source: std::io::Error },
    #[snafu(display("{source}"))]
    ArrowError { source: arrow2::error::Error },
    #[snafu(display("JSON deserialization error: {}", string))]
    JsonDeserializationError { string: String },
    #[snafu(display("Error chunking: {}", source))]
    ChunkError {
        source: TryChunksError<String, std::io::Error>,
    },
    #[snafu(display("Error joining spawned task: {}", source))]
    JoinError { source: tokio::task::JoinError },
    #[snafu(display(
        "Sender of OneShot Channel Dropped before sending data over: {}",
        source
    ))]
    OneShotRecvError {
        source: tokio::sync::oneshot::error::RecvError,
    },
    #[snafu(display("Error creating rayon threadpool: {}", source))]
    RayonThreadPoolError { source: rayon::ThreadPoolBuildError },
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

type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<JsonConvertOptions>()?;
    parent.add_class::<JsonParseOptions>()?;
    parent.add_class::<JsonReadOptions>()?;
    parent.add_wrapped(wrap_pyfunction!(python::pylib::read_json))?;
    parent.add_wrapped(wrap_pyfunction!(python::pylib::read_json_schema))?;
    Ok(())
}
