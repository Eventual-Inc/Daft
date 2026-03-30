use std::{fmt, time::SystemTime};

use uuid::Uuid;

/// Unique identifier for a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CheckpointId(Uuid);

impl CheckpointId {
    /// Generate a new random checkpoint ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a checkpoint ID from an existing UUID.
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID.
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "checkpoint-{}", self.0)
    }
}

/// Lifecycle state of a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckpointStatus {
    /// Keys and files are being accumulated. Not visible to readers.
    Staged,
    /// Sealed — keys and files are coupled and visible to readers.
    Checkpointed,
    /// Catalog commit succeeded. Files no longer returned by
    /// `get_checkpointed_files`, but keys remain visible.
    Committed,
}

impl fmt::Display for CheckpointStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Staged => write!(f, "staged"),
            Self::Checkpointed => write!(f, "checkpointed"),
            Self::Committed => write!(f, "committed"),
        }
    }
}

/// Metadata for a checkpoint (without keys/files payload).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub id: CheckpointId,
    pub status: CheckpointStatus,
    /// When the checkpoint entry was first created (first `stage_keys`/`stage_files` call).
    pub created_at: SystemTime,
    /// When the checkpoint was sealed via `checkpoint()`.
    pub sealed_at: Option<SystemTime>,
    /// When the checkpoint was marked committed via `mark_committed()`.
    pub committed_at: Option<SystemTime>,
}

impl Checkpoint {
    /// Create a new `Checkpoint` instance.
    #[must_use]
    pub fn new(
        id: CheckpointId,
        status: CheckpointStatus,
        created_at: SystemTime,
        sealed_at: Option<SystemTime>,
        committed_at: Option<SystemTime>,
    ) -> Self {
        Self {
            id,
            status,
            created_at,
            sealed_at,
            committed_at,
        }
    }
}

/// Tag indicating the format of the opaque file metadata blob.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileFormat {
    Iceberg,
    Parquet,
}

/// Opaque file metadata produced by a sink writer.
///
/// The data is serialized by the format-specific writer and deserialized
/// by the format-specific committer. The checkpoint store treats it as
/// an opaque blob.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    pub format: FileFormat,
    pub data: Vec<u8>,
}

impl FileMetadata {
    #[must_use]
    pub fn new(format: FileFormat, data: Vec<u8>) -> Self {
        Self { format, data }
    }
}
