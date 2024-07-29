#![feature(async_closure)]
#![feature(let_chains)]
#![feature(result_flattening)]

use common_error::DaftError;
use snafu::Snafu;

mod file;
pub mod metadata;
#[cfg(feature = "python")]
pub mod python;
pub mod read;
mod statistics;
pub use statistics::row_group_metadata_to_table_stats;
mod read_planner;
mod stream_reader;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    DaftIOError { source: daft_io::Error },

    #[snafu(display("Parquet reader timed out while trying to read: {path} with a time budget of {duration_ms} ms"))]
    FileReadTimeout { path: String, duration_ms: i64 },
    #[snafu(display("Internal IO Error when Opening: {path}:\nDetails:\n{source}"))]
    InternalIOError {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to parse parquet metadata for file {}: {}", path, source))]
    UnableToParseMetadata {
        path: String,
        source: parquet2::error::Error,
    },
    #[snafu(display("Unable to parse parquet metadata for file {}: {}", path, source))]
    UnableToParseMetadataFromLocalFile {
        path: String,
        source: arrow2::error::Error,
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
        "Unable to create arrow chunk from streaming file reader{}: {}",
        path,
        source
    ))]
    UnableToCreateChunkFromStreamingFileReader {
        path: String,
        source: arrow2::error::Error,
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
        "Unable to convert arrow schema to daft schema for file {}: {}",
        path,
        source
    ))]
    UnableToConvertSchemaToDaft { path: String, source: DaftError },

    #[snafu(display(
        "File: {} is not a valid parquet file. Has incorrect footer: {:?}",
        path,
        footer
    ))]
    InvalidParquetFile { path: String, footer: Vec<u8> },

    #[snafu(display(
        "File: {} is not a valid parquet file and is only {} bytes, smaller than the minimum size of 12 bytes",
        path,
        file_size
    ))]
    FileTooSmall { path: String, file_size: usize },

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
        "File: {} had a total of: {} row groups but requested index {}",
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
        "Parquet file: {} expected {} rows but only read: {} ",
        path,
        expected_rows,
        read_rows
    ))]
    ParquetNumRowMismatch {
        path: String,
        expected_rows: usize,
        read_rows: usize,
    },

    #[snafu(display(
        "Parquet file: {} has multiple columns with different number of rows",
        path,
    ))]
    ParquetColumnsDontHaveEqualRows { path: String },

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

    #[snafu(display(
        "Parquet file: {} attempted to delete row at position {} but only read {} rows",
        path,
        row,
        read_rows
    ))]
    ParquetDeleteRowOutOfIndex {
        path: String,
        row: usize,
        read_rows: usize,
    },

    #[snafu(display(
        "Parquet file: {} unable to convert row group metadata to stats\nDetails:\n{source}",
        path,
    ))]
    UnableToConvertRowGroupMetadataToStats { path: String, source: DaftError },

    #[snafu(display(
        "Parquet file: {} unable to evaluate predicate on stats\nDetails:\n{source}",
        path,
    ))]
    UnableToRunExpressionOnStats {
        path: String,
        source: daft_stats::Error,
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
            Error::DaftIOError { source } => source.into(),
            Error::FileReadTimeout { .. } => DaftError::ReadTimeout(err.into()),
            _ => DaftError::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for Error {
    fn from(err: daft_io::Error) -> Self {
        Error::DaftIOError { source: err }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;
