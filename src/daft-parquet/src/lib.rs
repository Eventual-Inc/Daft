use common_error::DaftError;
use snafu::Snafu;

mod helpers;
pub mod metadata;
mod metadata_adapter;
mod reader;
pub use metadata_adapter::{DaftParquetMetadata, DaftRowGroupMetaData, RowGroupList};
#[cfg(feature = "python")]
pub mod python;
pub mod read;
mod schema_inference;
mod statistics;
#[cfg(feature = "python")]
pub use python::register_modules;
pub use statistics::row_group_metadata_to_table_stats;

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

/// Errors raised while reading parquet files.
///
/// Variants are grouped by *what was happening when it failed*, not by which
/// upstream library produced the error. Each variant carries the file path so
/// messages tell you which file is at fault — except for pass-throughs of
/// errors that already carry their own URL context (`daft_io::Error`) and a
/// few low-level helpers that have no path in scope.
#[derive(Debug, Snafu)]
pub enum Error {
    // -- Pass-through wrappers --
    /// Errors from `daft_io::IOClient` (S3, GCS, HTTP, local-via-IOClient, ...).
    /// Source already carries URL context, so no path field here.
    #[snafu(display("{source}"))]
    IO { source: daft_io::Error },

    /// Errors from direct `std::fs` calls (our local-fastpath pread).
    #[snafu(display("Local IO error for {path}: {source}"))]
    LocalIO {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Spawned task failed for {path}: {source}"))]
    Join {
        path: String,
        source: tokio::task::JoinError,
    },

    // -- Parquet decode pipeline --
    #[snafu(display("Parquet metadata error for {path}: {source}"))]
    ParquetMetadata {
        path: String,
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to decode parquet column for {path}: {source}"))]
    ParquetColumnDecode {
        path: String,
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Arrow error for {path}: {source}"))]
    Arrow {
        path: String,
        source: arrow::error::ArrowError,
    },

    // -- Invalid parquet file (footer-time validation) --
    #[snafu(display("{path} is not a valid parquet file. Incorrect footer magic: {footer:?}"))]
    InvalidParquetFile { path: String, footer: Vec<u8> },

    #[snafu(display(
        "{path} is only {file_size} bytes, smaller than the minimum size of 12 bytes for a parquet file"
    ))]
    FileTooSmall { path: String, file_size: usize },

    #[snafu(display(
        "{path} reports a footer of {footer_size} bytes but the file is only {file_size} bytes"
    ))]
    InvalidParquetFooterSize {
        path: String,
        footer_size: usize,
        file_size: usize,
    },

    // -- Reader-internal invariant violations (programmer errors / unsupported schemas) --
    #[snafu(display("Parquet reader invariant violated for {path}: {message}"))]
    ReaderInternal { path: String, message: String },

    /// Remote fetch tasks store their results in a `Shared` future whose error
    /// type must be `Clone`, so we keep `Arc<Error>` internally and surface the
    /// typed source to every consumer.
    #[snafu(display("Remote parquet fetch failed for {path}: {source}"))]
    RemoteFetchFailed {
        path: String,
        source: std::sync::Arc<Error>,
    },

    // -- Top-level read concerns --
    #[snafu(display(
        "Parquet reader timed out while trying to read: {path} with a time budget of {duration_ms} ms"
    ))]
    FileReadTimeout { path: String, duration_ms: i64 },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        match err {
            // Pass daft_io errors through unchanged; they already carry URL context.
            Error::IO { source } => source.into(),
            // Bake the path into the std::io::Error message so downstream
            // consumers (which see only the DaftError::IoError source) still
            // know which file is at fault.
            Error::LocalIO { path, source } => Self::IoError(std::io::Error::new(
                source.kind(),
                format!("{path}: {source}"),
            )),
            // Lets retry logic key on `DaftError::ReadTimeout`.
            Error::FileReadTimeout { .. } => Self::ReadTimeout(err.into()),
            _ => Self::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for Error {
    fn from(err: daft_io::Error) -> Self {
        Self::IO { source: err }
    }
}

/// Maps a `DaftError` produced by `Runtime::spawn[_blocking]` /
/// `JoinSet::join_next` into a `crate::Error`. JoinError panics get the typed
/// `Error::Join` variant; anything else falls back to `Error::ReaderInternal`
/// with a formatted message. Returns a `FnOnce` so it composes with `map_err`.
pub(crate) fn task_err(path: impl Into<String>) -> impl FnOnce(common_error::DaftError) -> Error {
    let path = path.into();
    move |e| match e {
        common_error::DaftError::JoinError(source) => Error::Join { path, source },
        other => Error::ReaderInternal {
            path,
            message: format!("task join failed: {other}"),
        },
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;
