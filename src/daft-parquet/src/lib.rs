#![feature(async_closure)]

use common_error::DaftError;
use snafu::Snafu;

pub mod metadata;
pub mod read;
mod read_planner;
mod file;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open file {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: daft_io::Error,
    },

    #[snafu(display("Unable to read data from file {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: daft_io::Error,
    },

    #[snafu(display("Unable to parse parquet metadata for file {}: {}", path, source))]
    UnableToParseMetadata {
        path: String,
        source: parquet2::error::Error,
    },

    #[snafu(display("Unable to parse parquet metadata to arrow schema for file {}: {}", path, source))]
    UnableToParseSchemaFromMetadata {
        path: String,
        source: arrow2::error::Error,
    },
    #[snafu(display("Field: {} not found in Parquet File: {} Available Fields: {:?}", field, path, available_fields))]
    FieldNotFound {
        field: String,
        available_fields: Vec<String>,
        path: String
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

    #[snafu(display("Error joining spawned task: {} for path: {}", source, path))]
    JoinError {
        path: String,
        source: tokio::task::JoinError,
    },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        DaftError::External(err.into())
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;
