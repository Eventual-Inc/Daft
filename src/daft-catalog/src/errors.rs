use snafu::Snafu;

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

    #[snafu(display(
        "Invalid table name (expected only alphanumeric characters and '_'): {}",
        name
    ))]
    InvalidTableName { name: String },

    #[cfg(feature = "python")]
    #[snafu(display("Python error during {}: {}", context, source))]
    PythonError {
        source: pyo3::PyErr,
        context: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for common_error::DaftError {
    fn from(err: Error) -> Self {
        match &err {
            Error::TableNotFound { .. }
            | Error::CatalogNotFound { .. }
            | Error::InvalidTableName { .. } => {
                common_error::DaftError::CatalogError(err.to_string())
            }
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
