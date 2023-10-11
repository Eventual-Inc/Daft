use common_error::DaftError;
use snafu::Snafu;
pub(crate) mod column_stats;
mod micropartition;
mod table_stats;
mod utils;

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
