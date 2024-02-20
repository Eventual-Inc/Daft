#![feature(let_chains)]
use common_error::DaftError;
use snafu::Snafu;

mod column_stats;
mod partition_spec;
mod table_metadata;
mod table_stats;

pub use column_stats::{ColumnRangeStatistics, TruthValue};
pub use partition_spec::PartitionSpec;
pub use table_metadata::TableMetadata;
pub use table_stats::TableStatistics;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DaftCoreComputeError: {}", source))]
    DaftCoreCompute { source: DaftError },

    #[snafu(display("Duplicate name found when evaluating expressions: {}", name))]
    DuplicatedField { name: String },

    #[snafu(display("MissingStatistics: {}", source))]
    MissingStatistics { source: column_stats::Error },

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
