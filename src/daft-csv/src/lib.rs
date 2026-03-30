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
pub use options::{CsvConvertOptions, CsvParseOptions, CsvReadOptions, char_to_byte};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use read::{read_csv, read_csv_bulk, stream_csv};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    IOError { source: daft_io::Error },
    #[snafu(display("{source}"))]
    CSVError { source: csv_async::Error },
    #[snafu(display("{source}"))]
    SyncCSVError { source: csv::Error },
    #[snafu(display("Invalid char: {}", val))]
    WrongChar {
        source: std::char::TryFromCharError,
        val: char,
    },
    #[snafu(display("{source}"))]
    ArrowError { source: arrow_schema::ArrowError },
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
        // Inspect CSV-specific errors before falling through to External so that:
        // - Format errors (bad encoding, wrong field count) get DaftError::CsvError
        // - IO errors embedded inside csv_async/csv readers get DaftError::IoError
        //   rather than being lost inside External.
        let is_csv_format_err = match &err {
            Error::CSVError { source } => !source.is_io_error(),
            Error::SyncCSVError { source } => !matches!(source.kind(), csv::ErrorKind::Io(_)),
            _ => false,
        };
        if is_csv_format_err {
            return Self::CsvError(err.to_string());
        }

        match err {
            Error::IOError { source } => source.into(),
            Error::CSVError { source } => {
                // is_io_error() was true — extract the inner io::Error.
                if let csv_async::ErrorKind::Io(io_err) = source.into_kind() {
                    Self::IoError(io_err)
                } else {
                    unreachable!("is_io_error() returned true but kind is not Io")
                }
            }
            Error::SyncCSVError { source } => {
                // io kind check was true — extract the inner io::Error.
                if let csv::ErrorKind::Io(io_err) = source.into_kind() {
                    Self::IoError(io_err)
                } else {
                    unreachable!("csv io kind check was true but kind is not Io")
                }
            }
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
    parent.add_function(wrap_pyfunction!(python::pylib::read_csv, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::pylib::read_csv_schema, parent)?)?;
    Ok(())
}
