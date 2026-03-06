use common_error::DaftError;
use snafu::Snafu;

mod arrowrs_reader;
mod async_reader;
pub mod metadata;
mod metadata_adapter;
pub use metadata_adapter::{DaftParquetMetadata, DaftRowGroupMetaData, RowGroupList};
#[cfg(feature = "python")]
pub mod python;
pub mod read;
mod read_planner;
mod schema_inference;
mod statistics;
#[cfg(feature = "python")]
pub use python::register_modules;
pub use statistics::row_group_metadata_to_table_stats;

/// Infer a Daft `Schema` from arrow-rs-backed `DaftParquetMetadata`.
pub fn infer_schema_from_daft_metadata(
    metadata: &DaftParquetMetadata,
    options: read::ParquetSchemaInferenceOptions,
) -> common_error::DaftResult<daft_core::prelude::Schema> {
    let arrow_schema = schema_inference::infer_schema_from_parquet_metadata_arrowrs(
        metadata.as_arrowrs(),
        Some(options.coerce_int96_timestamp_unit),
        options.string_encoding == read::StringEncoding::Raw,
    )
    .map_err(|e| common_error::DaftError::External(e.into()))?;
    daft_core::prelude::Schema::try_from(&arrow_schema)
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("{source}"))]
    DaftIOError { source: daft_io::Error },

    #[snafu(display(
        "Parquet reader timed out while trying to read: {path} with a time budget of {duration_ms} ms"
    ))]
    FileReadTimeout { path: String, duration_ms: i64 },
    #[snafu(display("Internal IO Error when opening: {path}:\nDetails:\n{source}"))]
    InternalIOError {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Unable to parse parquet metadata (arrow-rs) for file {}: {}",
        path,
        source
    ))]
    UnableToParseMetadataArrowRs {
        path: String,
        source: parquet::errors::ParquetError,
    },
    #[snafu(display("Unable to parse parquet metadata for file {}: {}", path, source))]
    UnableToParseMetadataFromLocalFile {
        path: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("Unable to read parquet row group for file {}: {}", path, source))]
    UnableToReadParquetRowGroup {
        path: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display(
        "Unable to create table from arrow chunk for file {}: {}",
        path,
        source
    ))]
    UnableToCreateTableFromChunk { path: String, source: DaftError },
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

    #[snafu(display(
        "Parquet file: {} unable to bind expression to schema\nDetails:\n{source}",
        path,
    ))]
    UnableToBindExpression { path: String, source: DaftError },

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
    fn from(err: Error) -> Self {
        match err {
            Error::DaftIOError { source } => source.into(),
            Error::FileReadTimeout { .. } => Self::ReadTimeout(err.into()),
            Error::UnableToBindExpression { source, .. } => source.into(),
            _ => Self::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for Error {
    fn from(err: daft_io::Error) -> Self {
        Self::DaftIOError { source: err }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;
