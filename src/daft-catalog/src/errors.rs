use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to find specified table identifier: {}", table_id))]
    TableNotFoundError { table_id: String },
    // #[snafu(display("Failed to register catalog: {}", source))]
    // CatalogRegistrationError { source: Box<dyn std::error::Error + Send + Sync> },

    // #[snafu(display("Failed to register view: {}", source))]
    // ViewRegistrationError { source: Box<dyn std::error::Error + Send + Sync> },

    // #[snafu(display("Failed to read table: {}", source))]
    // TableReadError { source: Box<dyn std::error::Error + Send + Sync> },

    // #[snafu(display("Catalog not found: {}", name))]
    // CatalogNotFound { name: String },

    // #[snafu(display("Internal error: {}", source))]
    // InternalError {
    //     source: Box<dyn std::error::Error + Send + Sync>,
    // },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for common_error::DaftError {
    fn from(err: Error) -> Self {
        match &err {
            Error::TableNotFoundError { .. } => {
                common_error::DaftError::CatalogError(err.to_string())
            }
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
