use daft_common_error::DaftError;
use snafu::Snafu;

use crate::CheckpointId;

/// Checkpoint Result
pub type CheckpointResult<T, E = CheckpointError> = std::result::Result<T, E>;

/// Checkpoint Error
#[derive(Debug, Snafu)]
pub enum CheckpointError {
    #[snafu(display("Checkpoint {id} not found"))]
    CheckpointNotFound { id: CheckpointId },

    #[snafu(display("Checkpoint {id} is already sealed"))]
    AlreadySealed { id: CheckpointId },

    #[snafu(display("Checkpoint {id} is not yet checkpointed"))]
    NotCheckpointed { id: CheckpointId },

    #[snafu(display("{message}"))]
    Internal { message: String },

    #[snafu(display("{error}"))]
    DaftError { error: DaftError },
}

impl From<CheckpointError> for DaftError {
    fn from(err: CheckpointError) -> Self {
        Self::External(err.into())
    }
}

impl From<DaftError> for CheckpointError {
    fn from(error: DaftError) -> Self {
        Self::DaftError { error }
    }
}
