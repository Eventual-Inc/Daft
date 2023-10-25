use std::string::FromUtf8Error;

use common_error::DaftError;
use snafu::Snafu;

mod column_range;
mod table_stats;
mod utils;
pub use table_stats::row_group_metadata_to_table_stats;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(super) enum Error {
    #[snafu(display("MissingParquetColumnStatistics"))]
    MissingParquetColumnStatistics {},
    #[snafu(display("UnableToParseParquetColumnStatistics: {source}"))]
    UnableToParseParquetColumnStatistics { source: parquet2::error::Error },
    #[snafu(display("UnableToParseUtf8FromBinary: {source}"))]
    UnableToParseUtf8FromBinary { source: FromUtf8Error },
    #[snafu(display("DaftCoreComputeError: {}", source))]
    DaftCoreCompute { source: DaftError },
    #[snafu(display("DaftStatsError: {}", source))]
    DaftStats { source: daft_stats::Error },
}

impl From<daft_stats::Error> for Error {
    fn from(value: daft_stats::Error) -> Self {
        match value {
            daft_stats::Error::DaftCoreCompute { source } => Error::DaftCoreCompute { source },
            _ => Self::DaftStats { source: value },
        }
    }
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

pub(super) struct Wrap<T>(T);

impl<T> From<T> for Wrap<T> {
    fn from(value: T) -> Self {
        Wrap(value)
    }
}
