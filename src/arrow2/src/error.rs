//! Defines [`Error`], representing all errors returned by this crate.

use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Returned when functionality is not yet available.
    #[error("Not yet implemented: {0}")]
    NotYetImplemented(String),

    #[error("{0}")]
    Utf8Error(#[from] simdutf8::basic::Utf8Error),

    #[error("{0}")]
    StdUtf8Error(#[from] std::str::Utf8Error),

    #[error("{0}")]
    TryReserveError(#[from] std::collections::TryReserveError),

    /// Wrapper for an error triggered by a dependency
    #[error("External error{0}: {1}")]
    External(String, Box<dyn std::error::Error + Send + Sync>),

    /// Wrapper for IO errors
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),

    /// When an invalid argument is passed to a function.
    #[error("Invalid argument error: {0}")]
    InvalidArgumentError(String),

    /// Error during import or export to/from a format
    #[error("External format error: {0}")]
    ExternalFormat(String),

    /// Whenever pushing to a container fails because it does not support more entries.
    /// The solution is usually to use a higher-capacity container-backing type.
    #[error("Operation overflew the backing container.")]
    Overflow,

    /// Whenever incoming data from the C data interface, IPC or Flight does not fulfil the Arrow specification.
    #[error("{0}")]
    OutOfSpec(String),
}

impl Error {
    /// Wraps an external error in an `Error`.
    pub fn from_external_error(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::External("".to_string(), Box::new(error))
    }

    pub(crate) fn oos<A: Into<String>>(msg: A) -> Self {
        Self::OutOfSpec(msg.into())
    }

    #[allow(unused)]
    pub(crate) fn nyi<A: Into<String>>(msg: A) -> Self {
        Self::NotYetImplemented(msg.into())
    }
}

/// Typedef for a [`std::result::Result`] of an [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
