#![feature(let_chains)]
#![feature(iterator_try_reduce)]

use common_error::DaftError;
use snafu::Snafu;
mod micropartition;
mod ops;

pub use micropartition::MicroPartition;

#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DaftCoreComputeError: {}", source))]
    DaftCoreCompute { source: DaftError },

    #[cfg(feature = "python")]
    #[snafu(display("PyIOError: {}", source))]
    PyIO { source: PyErr },

    #[snafu(display("Duplicate name found when evaluating expressions: {}", name))]
    DuplicatedField { name: String },

    #[snafu(display("CSV error: {}", source))]
    DaftCSV { source: daft_csv::Error },

    #[snafu(display(
        "Field: {} not found in Parquet File:  Available Fields: {:?}",
        field,
        available_fields
    ))]
    FieldNotFound {
        field: String,
        available_fields: Vec<String>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        match value {
            Error::DaftCoreCompute { source } => source,
            _ => DaftError::External(value.into()),
        }
    }
}

#[cfg(feature = "python")]
impl From<Error> for pyo3::PyErr {
    fn from(value: Error) -> Self {
        let daft_error: DaftError = value.into();
        daft_error.into()
    }
}
