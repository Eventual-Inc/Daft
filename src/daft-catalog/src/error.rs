use snafu::Snafu;

use crate::{Identifier, Name};

/// Catalog Result
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Catalog Error
#[derive(Debug, Snafu)]
pub enum Error {
    // TODO remove me
    #[snafu(display(
        "Failed to find specified table identifier {} in the requested catalog {}",
        catalog_name,
        table_id
    ))]
    TableNotFound {
        catalog_name: String,
        table_id: String,
    },

    #[snafu(display("{typ_} with name \"{name}\" already exists!"))]
    ObjectAlreadyExists { typ_: String, name: String },

    #[snafu(display("{typ_} with name \"{name}\" not found!"))]
    ObjectNotFound { typ_: String, name: String },

    #[snafu(display("Invalid identifier {input}!"))]
    InvalidIdentifier { input: String },

    #[snafu(display("{message}"))]
    Unsupported { message: String },

    #[cfg(feature = "python")]
    #[snafu(display("Python error during {}: {}", context, source))]
    PythonError {
        source: pyo3::PyErr,
        context: String,
    },
}

impl Error {
    #[inline]
    pub fn unsupported<S: Into<String>>(message: S) -> Error {
        Error::Unsupported {
            message: message.into(),
        }
    }

    #[inline]
    pub fn obj_already_exists<S: Into<String>>(typ_: S, name: Name) -> Error {
        Error::ObjectAlreadyExists {
            typ_: typ_.into(),
            name: name.to_string(),
        }
    }

    // Consider typed arguments vs strings for consistent formatting.
    #[inline]
    pub fn obj_not_found<S: Into<String>>(typ_: S, ident: &Identifier) -> Error {
        Error::ObjectNotFound {
            typ_: typ_.into(),
            name: ident.to_string(),
        }
    }
}

impl From<Error> for common_error::DaftError {
    fn from(err: Error) -> Self {
        common_error::DaftError::CatalogError(err.to_string())
    }
}

#[cfg(feature = "python")]
use pyo3::PyErr;

#[cfg(feature = "python")]
impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        let daft_error: common_error::DaftError = value.into();
        daft_error.into()
    }
}
