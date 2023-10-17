#![feature(let_chains)]
#![allow(dead_code)]
#![feature(iterator_try_reduce)]

use common_error::DaftError;
use snafu::Snafu;
pub(crate) mod column_stats;
mod micropartition;
mod ops;
mod table_metadata;
mod table_stats;
mod utils;

#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DaftCoreComputeError: {}", source))]
    DaftCoreCompute { source: DaftError },
    #[snafu(display("MissingStatistics: {}", source))]
    MissingStatistics { source: column_stats::Error },
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
