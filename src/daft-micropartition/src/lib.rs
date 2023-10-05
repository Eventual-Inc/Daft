use common_error::DaftError;
use snafu::Snafu;

mod column_stats;
mod micropartition;
mod table_stats;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DaftCoreComputeError: {}", source))]
    DaftCoreCompute { source: DaftError },
}

type Result<T, E = Error> = std::result::Result<T, E>;
