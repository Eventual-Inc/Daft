//! Checkpoint store configuration types.
//!
//! This crate holds only the *configuration shapes* — serializable enums and
//! structs — that describe checkpoint stores. The live store implementations
//! (LocalFs, S3, InMemory, ...) live in `daft-checkpoint`. Keeping the configs
//! here lets `daft-logical-plan` reference `CheckpointStoreConfig` /
//! `CheckpointConfig` on `Source` nodes without pulling in the full store
//! crate or its transitive deps (Arrow, IO, etc.).
//!
//! Mirrors the `common/io-config` ↔ `daft-io` split.

#[cfg(feature = "python")]
pub mod python;

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Canonical column name used when persisting sealed keys to the checkpoint
/// store, and when reading them back at filter time.
///
/// Decouples the on-disk key file schema from the source's key column name —
/// re-running with a renamed source column does not invalidate previously
/// sealed key files, because they always live under this canonical name.
/// Callers that read the sealed key files (the scan operator) and the
/// optimizer rule that wires the anti-join must reference this constant
/// rather than the source's column name on the right side of the join.
pub const SEALED_KEYS_COLUMN: &str = "key";

/// Opaque identifier for a checkpoint.
///
/// The inner string is guaranteed to be safe for use as a path segment in
/// object-store keys (S3, GCS, local FS). Only ASCII alphanumeric characters,
/// hyphens (`-`), and underscores (`_`) are allowed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckpointId(String);

impl CheckpointId {
    /// Characters permitted in a checkpoint ID: ASCII alphanumeric, `-`, `_`.
    #[must_use]
    pub fn is_valid(s: &str) -> bool {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }

    /// Generate a new checkpoint ID for a pipeline input.
    #[must_use]
    pub fn generate(input_id: u32) -> Self {
        Self(format!("input-{input_id}-checkpoint-{}", Uuid::new_v4()))
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

/// Thread-safe map from `InputId` (`u32`) to `CheckpointId`.
///
/// Lazily generates a unique `CheckpointId` per input on first access.
/// Shared (via `Arc`) between `StageCheckpointKeysOperator` and
/// `BlockingSinkNode` so that both use the same ID for a given input.
#[derive(Debug, Clone)]
pub struct CheckpointIdMap(Arc<Mutex<HashMap<u32, CheckpointId>>>);

impl CheckpointIdMap {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    /// Return the `CheckpointId` for `input_id`, generating one if this is
    /// the first access for that input.
    pub fn get_or_generate(&self, input_id: u32) -> CheckpointId {
        self.0
            .lock()
            .expect("CheckpointIdMap lock poisoned")
            .entry(input_id)
            .or_insert_with(|| CheckpointId::generate(input_id))
            .clone()
    }
}

impl Default for CheckpointIdMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable configuration for constructing a live checkpoint store.
///
/// Each variant describes a backend kind. The corresponding live store
/// implementation is constructed via `daft_checkpoint::build_store(&config)`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CheckpointStoreConfig {
    /// Object-store backend (S3, GCS, Azure).
    ObjectStore {
        /// URI prefix (e.g. `s3://bucket/checkpoints`).
        prefix: String,
        /// IO configuration (endpoint, creds, region, etc.).
        io_config: Box<IOConfig>,
    },
}

/// Checkpoint configuration associated with a source node.
///
/// Specifies which store to use and which column identifies source records
/// for progress tracking.
///
/// TODO(DF-????): `key_column` is a plain column name (`String`) rather than
/// a full `ExprRef`. Accepting an expression would let callers use computed
/// or multi-column keys (e.g. `on=col("a") + col("b")`), but the only way to
/// express that in Rust is via `daft_dsl::ExprRef` — and pulling `daft-dsl`
/// into this "types-only" crate drags in `daft-core` (full Arrow, all array
/// ops), which defeats the purpose of the split (parity with
/// `common/io-config`, which has zero Arrow knowledge). Consulting with
/// the team on the right long-term shape — options include:
///   (a) accept the column-name restriction permanently;
///   (b) encode the expression as a serialized form (bytes / string DSL) here
///       and deserialize in consumers that already depend on daft-dsl;
///   (c) accept the daft-dsl dep and live with the transitive weight.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub store: CheckpointStoreConfig,
    pub key_column: String,
}
