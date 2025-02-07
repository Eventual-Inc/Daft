use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[macro_export]
macro_rules! unsupported {
    ($($arg:tt)*) => {
        return Err($crate::errors::Error::unsupported(format!($($arg)*)))
    };
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to find specified table identifier {} in the requested catalog {}",
        catalog_name,
        table_id
    ))]
    TableNotFound {
        catalog_name: String,
        table_id: String,
    },

    #[snafu(display("Catalog not found: {}", name))]
    CatalogNotFound { name: String },

    #[snafu(display("Invalid identifier `{input}`."))]
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
    pub fn unsupported(message: String) -> Error {
        Error::Unsupported { message }
    }
}

impl From<Error> for common_error::DaftError {
    fn from(err: Error) -> Self {
        match &err {
            Error::TableNotFound { .. }
            | Error::CatalogNotFound { .. }
            | Error::InvalidIdentifier { .. }
            | Error::Unsupported { .. } => common_error::DaftError::CatalogError(err.to_string()),
            #[cfg(feature = "python")]
            Error::PythonError { .. } => common_error::DaftError::CatalogError(err.to_string()),
        }
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
