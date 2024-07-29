#![feature(async_closure)]
#![feature(let_chains)]
#![feature(trait_alias)]
#![feature(trait_upcasting)]
use common_error::DaftError;
use snafu::Snafu;

pub mod metadata;
pub mod options;
#[cfg(feature = "python")]
pub mod python;
pub mod read;
mod schema;

pub use metadata::read_csv_schema_bulk;
pub use options::{char_to_byte, CsvConvertOptions, CsvParseOptions, CsvReadOptions};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use read::{read_csv, read_csv_bulk, stream_csv};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    IOError { source: daft_io::Error },
    #[snafu(display("{source}"))]
    CSVError { source: csv_async::Error },
    #[snafu(display("Invalid char: {}", val))]
    WrongChar {
        source: std::char::TryFromCharError,
        val: char,
    },
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

#[cfg(feature = "python")]
impl From<Error> for pyo3::PyErr {
    fn from(value: Error) -> Self {
        let daft_error: DaftError = value.into();
        daft_error.into()
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<CsvConvertOptions>()?;
    parent.add_class::<CsvParseOptions>()?;
    parent.add_class::<CsvReadOptions>()?;
    parent.add_wrapped(wrap_pyfunction!(python::pylib::read_csv))?;
    parent.add_wrapped(wrap_pyfunction!(python::pylib::read_csv_schema))?;
    Ok(())
}
