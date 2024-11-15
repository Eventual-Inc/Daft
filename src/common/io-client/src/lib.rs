#![feature(if_let_guard)]
#![feature(io_error_more)]
#![feature(let_chains)]
mod object_source;
mod stats;
mod local;
mod object_store_glob;
mod io_client;
pub mod stream_utils;
#[cfg(feature = "python")]
pub mod python;

use std::sync::Arc;

use common_error::DaftError;
use snafu::Snafu;

pub use object_source::{ObjectSource, ObjectSourceFactory, ObjectSourceFactoryEntry, FileType};
pub use io_client::{IOClient, get_io_client};
pub use stats::{IOStatsRef, IOStatsContext};
pub use object_source::{GetResult, LSResult, FileMetadata};
pub use common_io_config::IOConfig;
pub use common_file_formats::FileFormat;
pub use object_store_glob::glob as glob_util;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generic {} error: {}", store, source))]
    Generic { store: SourceType, source: DynError },
    #[snafu(display("Object at location {} not found\nDetails:\n{}", path, source))]
    NotFound { path: String, source: DynError },

    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },

    #[snafu(display("Unable to expand home dir"))]
    HomeDirError { path: String },

    #[snafu(display("Unable to open file {}: {:?}", path, source))]
    UnableToOpenFile { path: String, source: DynError },

    #[snafu(display("Unable to create directory {}: {:?}", path, source))]
    UnableToCreateDir {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to read data from file {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write data to file {}: {}", path, source))]
    UnableToWriteToFile {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Connection timed out when trying to connect to {}\nDetails:\n{:?}",
        path,
        source
    ))]
    ConnectTimeout { path: String, source: DynError },

    #[snafu(display("Read timed out when trying to read {}\nDetails:\n{:?}", path, source))]
    ReadTimeout { path: String, source: DynError },

    #[snafu(display(
        "Socket error occurred when trying to read {}\nDetails:\n{:?}",
        path,
        source
    ))]
    SocketError { path: String, source: DynError },

    #[snafu(display("Throttled when trying to read {}\nDetails:\n{:?}", path, source))]
    Throttled { path: String, source: DynError },

    #[snafu(display("Misc Transient error trying to read {}\nDetails:\n{:?}", path, source))]
    MiscTransient { path: String, source: DynError },

    #[snafu(display("Unable to convert URL \"{}\" to path", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },

    #[snafu(display("Not a File: \"{}\"", path))]
    NotAFile { path: String },

    #[snafu(display("Unable to determine size of {}", path))]
    UnableToDetermineSize { path: String },

    #[snafu(display("Unable to load Credentials for store: {store}\nDetails:\n{source:?}"))]
    UnableToLoadCredentials { store: SourceType, source: DynError },

    #[snafu(display("Failed to load Credentials for store: {store}\nDetails:\n{source:?}"))]
    UnableToCreateClient { store: SourceType, source: DynError },

    #[snafu(display("Unauthorized to access store: {store} for file: {path}\nYou may need to set valid Credentials\n{source}"))]
    Unauthorized {
        store: SourceType,
        path: String,
        source: DynError,
    },

    #[snafu(display("Source not yet implemented: {}", store))]
    NotImplementedSource { store: String },

    #[snafu(display("Unhandled Error for path: {}\nDetails:\n{}", path, msg))]
    Unhandled { path: String, msg: String },

    #[snafu(
        display("Error sending data over a tokio channel: {}", source),
        context(false)
    )]
    UnableToSendDataOverChannel { source: DynError },

    #[snafu(display("Error joining spawned task: {}", source), context(false))]
    JoinError { source: tokio::task::JoinError },

    #[snafu(display("Cached error: {}", source))]
    CachedError { source: Arc<Error> },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        use Error::{
            CachedError, ConnectTimeout, MiscTransient, NotFound, ReadTimeout, SocketError,
            Throttled, UnableToReadBytes,
        };
        match err {
            NotFound { path, source } => Self::FileNotFound { path, source },
            ConnectTimeout { .. } => Self::ConnectTimeout(err.into()),
            ReadTimeout { .. } => Self::ReadTimeout(err.into()),
            UnableToReadBytes { .. } => Self::ByteStreamError(err.into()),
            SocketError { .. } => Self::SocketError(err.into()),
            Throttled { .. } => Self::ThrottledIo(err.into()),
            MiscTransient { .. } => Self::MiscTransient(err.into()),
            // We have to repeat everything above for the case we have an Arc since we can't move the error.
            CachedError { ref source } => match source.as_ref() {
                NotFound { path, source: _ } => Self::FileNotFound {
                    path: path.clone(),
                    source: err.into(),
                },
                ConnectTimeout { .. } => Self::ConnectTimeout(err.into()),
                ReadTimeout { .. } => Self::ReadTimeout(err.into()),
                UnableToReadBytes { .. } => Self::ByteStreamError(err.into()),
                SocketError { .. } => Self::SocketError(err.into()),
                Throttled { .. } => Self::ThrottledIo(err.into()),
                MiscTransient { .. } => Self::MiscTransient(err.into()),
                _ => Self::External(err.into()),
            },
            _ => Self::External(err.into()),
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        Self::new(std::io::ErrorKind::Other, err)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Hash, PartialEq, std::cmp::Eq, Clone, Copy, derive_more::Display)]
pub enum SourceType {
    File,
    Http,
    S3,
    AzureBlob,
    GCS,
    HF,
}

