use std::{fmt, time::SystemTime};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Opaque identifier for a checkpoint.
///
/// The inner string is guaranteed to be safe for use as a path segment in
/// object-store keys (S3, GCS, local FS). Only ASCII alphanumeric characters,
/// hyphens (`-`), and underscores (`_`) are allowed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckpointId(String);

impl CheckpointId {
    /// Characters permitted in a checkpoint ID: ASCII alphanumeric, `-`, `_`.
    pub(crate) fn is_valid(s: &str) -> bool {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }

    /// Generate a new checkpoint ID associated with a task.
    #[must_use]
    pub fn generate(task_id: u32) -> Self {
        Self(format!("task-{task_id}-checkpoint-{}", Uuid::new_v4()))
    }

    /// Reconstruct a checkpoint ID from a previously serialized string.
    ///
    /// # Panics
    ///
    /// Panics if `s` is empty or contains characters outside the allowed set
    /// (ASCII alphanumeric, `-`, `_`).
    #[must_use]
    pub fn from_string(s: String) -> Self {
        assert!(
            Self::is_valid(&s),
            "CheckpointId must be non-empty and contain only ASCII alphanumeric, '-', or '_' characters, got: {s:?}"
        );
        Self(s)
    }
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<CheckpointId> for String {
    fn from(id: CheckpointId) -> Self {
        id.0
    }
}

impl AsRef<str> for CheckpointId {
    fn as_ref(&self) -> &str {
        &self.0
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_valid_id() {
        let id = CheckpointId::generate(42);
        assert!(CheckpointId::is_valid(id.as_ref()));
    }

    #[test]
    fn from_string_accepts_valid_id() {
        let id = CheckpointId::from_string("task-0-checkpoint-abc123".to_string());
        assert_eq!(id.as_ref(), "task-0-checkpoint-abc123");
    }

    #[test]
    #[should_panic(expected = "CheckpointId must be non-empty")]
    fn from_string_rejects_empty() {
        let _ = CheckpointId::from_string(String::new());
    }

    #[test]
    #[should_panic(expected = "CheckpointId must be non-empty and contain only ASCII alphanumeric")]
    fn from_string_rejects_slash() {
        let _ = CheckpointId::from_string("bad/path".to_string());
    }

    #[test]
    #[should_panic(expected = "CheckpointId must be non-empty and contain only ASCII alphanumeric")]
    fn from_string_rejects_space() {
        let _ = CheckpointId::from_string("bad path".to_string());
    }
}
