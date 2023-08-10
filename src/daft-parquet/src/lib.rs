#![feature(async_closure)]

use common_error::DaftError;
use snafu::Snafu;

mod file;
pub mod metadata;
#[cfg(feature = "python")]
pub mod python;
pub mod read;
mod read_planner;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    IOError { source: daft_io::Error },

    #[snafu(display("Unable to parse parquet metadata for file {}: {}", path, source))]
    UnableToParseMetadata {
        path: String,
        source: parquet2::error::Error,
    },

    #[snafu(display(
        "Unable to create arrow arrays from parquet pages {}: {}",
        path,
        source
    ))]
    UnableToConvertParquetPagesToArrow {
        path: String,
        source: arrow2::error::Error,
    },

    #[snafu(display("Unable to create page stream for parquet file {}: {}", path, source))]
    UnableToCreateParquetPageStream {
        path: String,
        source: parquet2::error::Error,
    },

    #[snafu(display(
        "Unable to parse parquet metadata to arrow schema for file {}: {}",
        path,
        source
    ))]
    UnableToParseSchemaFromMetadata {
        path: String,
        source: arrow2::error::Error,
    },
    #[snafu(display(
        "Field: {} not found in Parquet File: {} Available Fields: {:?}",
        field,
        path,
        available_fields
    ))]
    FieldNotFound {
        field: String,
        available_fields: Vec<String>,
        path: String,
    },
    #[snafu(display(
        "File: {} is not a valid parquet file. Has incorrect footer: {:?}",
        path,
        footer
    ))]
    InvalidParquetFile { path: String, footer: Vec<u8> },
    #[snafu(display(
        "File: {} has a footer size: {} greater than the file size: {}",
        path,
        footer_size,
        file_size
    ))]
    InvalidParquetFooterSize {
        path: String,
        footer_size: usize,
        file_size: usize,
    },

    #[snafu(display(
        "File: {} had a total of: {} row groups but requested {}",
        path,
        total_row_groups,
        row_group
    ))]
    ParquetRowGroupOutOfIndex {
        path: String,
        row_group: i64,
        total_row_groups: i64,
    },

    #[snafu(display(
        "Parquet file: {} metadata listed {} rows but only read: {} ",
        path,
        metadata_num_rows,
        read_rows
    ))]
    ParquetNumRowMismatch {
        path: String,
        metadata_num_rows: usize,
        read_rows: usize,
    },

    #[snafu(display(
        "Parquet file: {} metadata listed {} columns but only read: {} ",
        path,
        metadata_num_columns,
        read_columns
    ))]
    ParquetNumColumnMismatch {
        path: String,
        metadata_num_columns: usize,
        read_columns: usize,
    },

    #[snafu(display("Error joining spawned task: {} for path: {}", source, path))]
    JoinError {
        path: String,
        source: tokio::task::JoinError,
    },
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

type Result<T, E = Error> = std::result::Result<T, E>;
