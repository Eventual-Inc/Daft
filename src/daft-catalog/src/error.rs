use snafu::Snafu;

use crate::Identifier;

/// Catalog Result
pub type CatalogResult<T, E = CatalogError> = std::result::Result<T, E>;

/// Catalog Error
#[derive(Debug, Snafu)]
pub enum CatalogError {
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

    #[snafu(display("{type_} with name {ident} already exists!"))]
    ObjectAlreadyExists { type_: String, ident: String },

    #[snafu(display("{type_} with name {ident} not found!"))]
    ObjectNotFound { type_: String, ident: String },

    #[snafu(display("Ambigiuous identifier for {input}: found `{options}`!"))]
    AmbiguousIdentifier { input: String, options: String },

    #[snafu(display("Invalid identifier {input}!"))]
    InvalidIdentifier { input: String },

    #[snafu(display("{message}"))]
    Unsupported { message: String },

    #[cfg(feature = "python")]
    #[snafu(display("Python error: {}", source))]
    PythonError { source: pyo3::PyErr },
}

impl CatalogError {
    #[inline]
    pub fn unsupported<S: Into<String>>(message: S) -> CatalogError {
        CatalogError::Unsupported {
            message: message.into(),
        }
    }

    #[inline]
    pub fn obj_already_exists<S: Into<String>>(type_: S, ident: &Identifier) -> CatalogError {
        CatalogError::ObjectAlreadyExists {
            type_: type_.into(),
            ident: ident.to_string(),
        }
    }

    // Consider typed arguments vs strings for consistent formatting.
    #[inline]
    pub fn obj_not_found<S: Into<String>>(typ_: S, ident: &Identifier) -> CatalogError {
        CatalogError::ObjectNotFound {
            type_: typ_.into(),
            ident: ident.to_string(),
        }
    }

    pub fn ambiguous_identifier<O, I>(input: I, options: O) -> Self
    where
        O: IntoIterator<Item = I>,
        I: Into<String>,
    {
        CatalogError::AmbiguousIdentifier {
            input: input.into(),
            options: options
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    #[inline]
    pub fn invalid_identifier<S: Into<String>>(input: S) -> CatalogError {
        CatalogError::InvalidIdentifier {
            input: input.into(),
        }
    }
}

impl From<CatalogError> for common_error::DaftError {
    fn from(err: CatalogError) -> Self {
        common_error::DaftError::CatalogError(err.to_string())
    }
}

#[cfg(feature = "python")]
use pyo3::PyErr;

#[cfg(feature = "python")]
impl From<CatalogError> for PyErr {
    fn from(value: CatalogError) -> Self {
        let daft_error: common_error::DaftError = value.into();
        daft_error.into()
    }
}

#[cfg(feature = "python")]
impl From<PyErr> for CatalogError {
    fn from(value: PyErr) -> Self {
        CatalogError::PythonError { source: value }
    }
}
