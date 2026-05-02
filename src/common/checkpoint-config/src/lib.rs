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

use common_hashable_float_wrapper::FloatWrapper;
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

/// How checkpoint keys are derived from source data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CheckpointKeyMode {
    /// Keys are values of a named column in the source schema.
    /// Requires an anti-join against previously-sealed keys at execution time.
    RowLevel { key_column: String },
    /// Keys are derived from scan-task metadata (file path + chunk spec).
    /// No anti-join needed — filtering uses a HashSet lookup in ScanTaskSource.
    FilePath,
}

/// Checkpoint configuration associated with a source node.
///
/// Specifies which store to use and how source records are identified
/// for progress tracking.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub store: CheckpointStoreConfig,
    pub key_mode: CheckpointKeyMode,
    pub settings: CheckpointSettings,
}

impl CheckpointConfig {
    /// Returns the key column name if this is a row-level checkpoint.
    #[must_use]
    pub fn key_column(&self) -> Option<&str> {
        match &self.key_mode {
            CheckpointKeyMode::RowLevel { key_column } => Some(key_column),
            CheckpointKeyMode::FilePath => None,
        }
    }

    #[must_use]
    pub fn is_file_path_mode(&self) -> bool {
        matches!(self.key_mode, CheckpointKeyMode::FilePath)
    }
}

/// Strategy-specific settings for skipping already-checkpointed source rows.
///
/// Each variant carries the tuning knobs for one filter strategy. Today the
/// only strategy is anti-join via `KeyFiltering`; future variants (bloom
/// filter, hash partition, etc.) would slot in alongside.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CheckpointSettings {
    KeyFiltering(KeyFilteringSettings),
}

impl Default for CheckpointSettings {
    fn default() -> Self {
        Self::KeyFiltering(KeyFilteringSettings::default())
    }
}

/// Tuning knobs for the key-filtering anti-join used to skip already-sealed
/// source rows. `None` for any field means "use the engine default".
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyFilteringSettings {
    pub num_workers: Option<usize>,
    pub cpus_per_worker: Option<FloatWrapper<f64>>,
    pub keys_load_batch_size: Option<usize>,
    pub max_concurrency_per_worker: Option<usize>,
    pub filter_batch_size: Option<usize>,
}

/// Thread-safe registry mapping `InputId` to source atom keys for
/// file-path checkpoint mode.
///
/// During scan-task processing, atom keys (file paths with optional chunk
/// suffixes) are registered for each InputId. At seal time, the sink takes
/// the accumulated keys and stages them in the checkpoint store.
#[derive(Debug, Clone)]
pub struct FilePathRegistry(Arc<Mutex<HashMap<u32, Vec<String>>>>);

impl FilePathRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    pub fn register(&self, input_id: u32, atom_keys: Vec<String>) {
        self.0
            .lock()
            .expect("FilePathRegistry lock poisoned")
            .entry(input_id)
            .or_default()
            .extend(atom_keys);
    }

    pub fn take(&self, input_id: u32) -> Vec<String> {
        self.0
            .lock()
            .expect("FilePathRegistry lock poisoned")
            .remove(&input_id)
            .unwrap_or_default()
    }
}

impl Default for FilePathRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_store() -> CheckpointStoreConfig {
        CheckpointStoreConfig::ObjectStore {
            prefix: "s3://test/ckpt".to_string(),
            io_config: Box::new(IOConfig::default()),
        }
    }

    fn make_config(key_mode: CheckpointKeyMode) -> CheckpointConfig {
        CheckpointConfig {
            store: dummy_store(),
            key_mode,
            settings: CheckpointSettings::default(),
        }
    }

    #[test]
    fn row_level_key_column_returns_name() {
        let cfg = make_config(CheckpointKeyMode::RowLevel {
            key_column: "file_id".to_string(),
        });
        assert_eq!(cfg.key_column(), Some("file_id"));
        assert!(!cfg.is_file_path_mode());
    }

    #[test]
    fn file_path_key_column_returns_none() {
        let cfg = make_config(CheckpointKeyMode::FilePath);
        assert_eq!(cfg.key_column(), None);
        assert!(cfg.is_file_path_mode());
    }

    #[test]
    fn file_path_registry_register_and_take() {
        let registry = FilePathRegistry::new();
        registry.register(0, vec!["a.parquet".into(), "b.parquet".into()]);
        registry.register(0, vec!["c.parquet".into()]);
        registry.register(1, vec!["d.parquet".into()]);

        let keys_0 = registry.take(0);
        assert_eq!(keys_0, vec!["a.parquet", "b.parquet", "c.parquet"]);

        let keys_1 = registry.take(1);
        assert_eq!(keys_1, vec!["d.parquet"]);
    }

    #[test]
    fn file_path_registry_take_returns_empty_for_unknown_input() {
        let registry = FilePathRegistry::new();
        assert!(registry.take(42).is_empty());
    }

    #[test]
    fn file_path_registry_take_drains() {
        let registry = FilePathRegistry::new();
        registry.register(0, vec!["x.parquet".into()]);
        let _ = registry.take(0);
        assert!(registry.take(0).is_empty());
    }

    #[test]
    fn file_path_registry_concurrent_access() {
        let registry = FilePathRegistry::new();
        let r1 = registry.clone();
        let r2 = registry.clone();

        let t1 = std::thread::spawn(move || {
            for i in 0..100 {
                r1.register(0, vec![format!("t1-{i}")]);
            }
        });
        let t2 = std::thread::spawn(move || {
            for i in 0..100 {
                r2.register(0, vec![format!("t2-{i}")]);
            }
        });
        t1.join().unwrap();
        t2.join().unwrap();

        let keys = registry.take(0);
        assert_eq!(keys.len(), 200);
    }
}

impl KeyFilteringSettings {
    /// Validate that all numeric fields are strictly positive when set.
    ///
    /// # Errors
    ///
    /// Returns `Err(message)` if any field is `Some(0)` or `cpus_per_worker`
    /// is `Some(v)` with `v <= 0.0`.
    pub fn validate(&self) -> Result<(), String> {
        if matches!(self.num_workers, Some(0)) {
            return Err("[checkpoint] num_workers must be > 0".to_string());
        }
        if matches!(self.cpus_per_worker, Some(FloatWrapper(v)) if v <= 0.0) {
            return Err("[checkpoint] cpus_per_worker must be > 0".to_string());
        }
        if matches!(self.keys_load_batch_size, Some(0)) {
            return Err("[checkpoint] keys_load_batch_size must be > 0".to_string());
        }
        if matches!(self.max_concurrency_per_worker, Some(0)) {
            return Err("[checkpoint] max_concurrency_per_worker must be > 0".to_string());
        }
        if matches!(self.filter_batch_size, Some(0)) {
            return Err("[checkpoint] filter_batch_size must be > 0".to_string());
        }
        Ok(())
    }
}
