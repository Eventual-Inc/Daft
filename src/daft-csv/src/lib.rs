#![feature(async_closure)]
#![feature(let_chains)]
#![feature(new_uninit)]
#![feature(trait_alias)]
#![feature(trait_upcasting)]
#![feature(test)]
extern crate test;
use common_error::DaftError;
use snafu::Snafu;

pub mod local;
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
    fn from(err: Error) -> Self {
        match err {
            Error::IOError { source } => source.into(),
            _ => Self::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for Error {
    fn from(err: daft_io::Error) -> Self {
        Self::IOError { source: err }
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
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<CsvConvertOptions>()?;
    parent.add_class::<CsvParseOptions>()?;
    parent.add_class::<CsvReadOptions>()?;
    parent.add_function(wrap_pyfunction_bound!(python::pylib::read_csv, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        python::pylib::read_csv_schema,
        parent
    )?)?;
    Ok(())
}
