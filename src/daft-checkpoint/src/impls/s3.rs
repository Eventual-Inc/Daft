use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use daft_core::series::Series;
use daft_io::{IOConfig, get_io_client};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, future::try_join_all, stream::BoxStream};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, FileMetadata,
    error::{CheckpointError, CheckpointResult},
    types::FileFormat,
};

fn rfc3339_now() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn rfc3339_from(t: SystemTime) -> String {
    DateTime::<Utc>::from(t)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

fn system_time_from_rfc3339(s: &str) -> SystemTime {
    s.parse::<DateTime<Utc>>()
        .map(SystemTime::from)
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

/// Written to `{prefix}/{id}/manifest.json` by `checkpoint()`.
/// Presence of this file is the atomic visibility boundary — once written, never overwritten.
#[derive(Serialize, Deserialize)]
struct Manifest {
    checkpoint_id: String,
    created_at: String,
    sealed_at: String,
    num_key_files: usize,
    num_file_files: usize,
}

/// Written to `{prefix}/committed_log/{uuid}.json` by `mark_committed()`.
///
/// One file per call — append-only log. This makes `mark_committed(ids)` a single atomic
/// `PutObject`: crash before → nothing recorded; crash after → all IDs marked.
#[derive(Serialize, Deserialize)]
struct CommittedLogEntry {
    committed_checkpoint_ids: Vec<String>,
}

struct StagedEntry {
    keys: Vec<Series>,
    files: Vec<FileMetadata>,
    created_at: SystemTime,
}

impl StagedEntry {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            files: Vec::new(),
            created_at: SystemTime::now(),
        }
    }
}

/// Controls what happens to checkpoint data after a successful job run.
#[derive(Debug, Clone)]
pub enum CleanupPolicy {
    /// No automatic cleanup. The operator is responsible for deleting the prefix
    /// when it is no longer needed (e.g., `aws s3 rm --recursive s3://bucket/prefix/`
    /// after each successful run). For time-based expiry, an S3 lifecycle rule on
    /// the prefix can also be configured out-of-band. This is the default.
    Manual,

    /// After a successful [`mark_committed()`](CheckpointStore::mark_committed),
    /// Daft deletes the entire checkpoint prefix. Safe because checkpoint data
    /// loses its value once the job completes — any re-run creates fresh checkpoints.
    DeleteAfterSuccess,
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        Self::Manual
    }
}

/// S3-backed [`CheckpointStore`].
///
/// # Layout
///
/// ```text
/// {prefix}/
///   {checkpoint_id}/
///     keys/
///       0000.ipc          ← Arrow IPC; one file per stage_keys() call
///       ...
///     files/
///       0000.bin          ← FileMetadata blob; one file per staged file
///       ...
///     manifest.json       ← written last by checkpoint(); presence = sealed
///   committed_log/
///     {uuid}.json         ← { "committed_checkpoint_ids": [...] }
///     ...                 ← one file per mark_committed() call (append-only log)
/// ```
///
/// # Crash semantics
///
/// - **Staging** is in-memory only — a crash before `checkpoint()` leaves no trace.
/// - **`checkpoint()`** writes keys then files then `manifest.json` last. Orphaned
///   key/file objects from an interrupted write are harmless — they are never read
///   because their `manifest.json` was never written.
/// - **`mark_committed()`** is a single atomic `PutObject` to `committed_log/`.
///
/// # Consistency
///
/// AWS S3 offers strong read-after-write consistency for PUTs and LISTs (since
/// December 2020). S3-compatible stores (MinIO, Ceph, GCS) may have weaker LIST
/// guarantees — verify your backend before relying on listing correctness.
pub struct S3CheckpointStore {
    prefix: String,
    io_config: Arc<IOConfig>,
    /// Never held across `.await` points.
    staged: RwLock<HashMap<CheckpointId, StagedEntry>>,
}

impl S3CheckpointStore {
    /// Create a new checkpoint store rooted at `prefix` (trailing slashes stripped).
    pub fn new(prefix: impl Into<String>, io_config: Arc<IOConfig>) -> Self {
        Self {
            prefix: prefix.into().trim_end_matches('/').to_string(),
            io_config,
            staged: RwLock::new(HashMap::new()),
        }
    }

    /// Delete all objects under the checkpoint prefix and clear in-memory staged entries.
    /// Called automatically by [`CleanupPolicy::DeleteAfterSuccess`], but can also be
    /// invoked explicitly regardless of policy.
    pub async fn cleanup(&self) -> CheckpointResult<()> {
        self.staged
            .write()
            .map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?
            .clear();

        self.io_client()?
            .delete_dir(&self.prefix, None)
            .await
            .map_err(|e| CheckpointError::Internal {
                message: format!("cleanup of prefix {} failed: {e}", self.prefix),
            })
    }

    fn manifest_path(&self, id: CheckpointId) -> String {
        format!("{}/{}/manifest.json", self.prefix, id.as_uuid())
    }

    fn key_path(&self, id: CheckpointId, idx: usize) -> String {
        format!("{}/{}/keys/{:04}.ipc", self.prefix, id.as_uuid(), idx)
    }

    fn file_path(&self, id: CheckpointId, idx: usize) -> String {
        format!("{}/{}/files/{:04}.bin", self.prefix, id.as_uuid(), idx)
    }

    fn manifests_glob(&self) -> String {
        format!("{}/*/manifest.json", self.prefix)
    }

    fn committed_log_glob(&self) -> String {
        format!("{}/committed_log/*.json", self.prefix)
    }

    fn new_committed_log_path(&self) -> String {
        format!("{}/committed_log/{}.json", self.prefix, Uuid::new_v4())
    }

    fn io_client(&self) -> CheckpointResult<Arc<daft_io::IOClient>> {
        get_io_client(true, self.io_config.clone()).map_err(|e| CheckpointError::Internal {
            message: format!("failed to create IO client: {e}"),
        })
    }

    async fn put_bytes(&self, path: &str, data: Vec<u8>) -> CheckpointResult<()> {
        // S3 is a flat key-value store; local paths need parent dirs created explicitly.
        if let Some(local) = path.strip_prefix("file://")
            && let Some(parent) = std::path::Path::new(local).parent()
        {
            std::fs::create_dir_all(parent).map_err(|e| CheckpointError::Internal {
                message: format!("failed to create directory {}: {e}", parent.display()),
            })?;
        }
        self.io_client()?
            .single_url_put(path, Bytes::from(data), None)
            .await
            .map_err(|e| CheckpointError::Internal {
                message: format!("PUT {path} failed: {e}"),
            })
    }

    async fn get_bytes(&self, path: &str) -> CheckpointResult<Bytes> {
        let result = self
            .io_client()?
            .single_url_get(path.to_string(), None, None)
            .await
            .map_err(|e| CheckpointError::Internal {
                message: format!("GET {path} failed: {e}"),
            })?;
        result.bytes().await.map_err(|e| CheckpointError::Internal {
            message: format!("reading bytes from {path} failed: {e}"),
        })
    }

    /// Returns empty vec if the prefix doesn't exist yet. Propagates other errors
    /// (permission failures, network errors, malformed URLs).
    async fn glob_paths(&self, pattern: &str) -> CheckpointResult<Vec<String>> {
        let client = self.io_client()?;
        match client
            .glob(pattern.to_string(), None, None, None, None, None)
            .await
        {
            Ok(stream) => Ok(stream
                .filter_map(|r| async move { r.ok() })
                .map(|fm| fm.filepath)
                .collect()
                .await),
            Err(daft_io::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(CheckpointError::Internal {
                message: format!("glob {pattern} failed: {e}"),
            }),
        }
    }

    fn series_to_ipc(series: &Series) -> CheckpointResult<Vec<u8>> {
        let batch = RecordBatch::from_nonempty_columns(vec![series.clone()]).map_err(|e| {
            CheckpointError::Internal {
                message: format!("failed to build RecordBatch from Series: {e}"),
            }
        })?;
        batch
            .to_ipc_stream()
            .map_err(|e| CheckpointError::Internal {
                message: format!("failed to serialize Series to IPC: {e}"),
            })
    }

    fn ipc_to_series(bytes: &[u8]) -> CheckpointResult<Series> {
        let batch = RecordBatch::from_ipc_stream(bytes).map_err(|e| CheckpointError::Internal {
            message: format!("failed to deserialize Series from IPC: {e}"),
        })?;
        if batch.num_columns() == 0 {
            return Err(CheckpointError::Internal {
                message: "IPC batch has no columns".to_string(),
            });
        }
        Ok(batch.get_column(0).clone())
    }

    /// Encoding: `[format_tag: u8, data...]`. Tags: `0` = Iceberg, `1` = Parquet.
    fn encode_file_metadata(f: &FileMetadata) -> CheckpointResult<Vec<u8>> {
        let tag = match f.format {
            FileFormat::Iceberg => 0u8,
            FileFormat::Parquet => 1u8,
        };
        let mut out = Vec::with_capacity(1 + f.data.len());
        out.push(tag);
        out.extend_from_slice(&f.data);
        Ok(out)
    }

    fn decode_file_metadata(bytes: &[u8]) -> CheckpointResult<FileMetadata> {
        if bytes.is_empty() {
            return Err(CheckpointError::Internal {
                message: "empty file metadata blob".to_string(),
            });
        }
        let format = match bytes[0] {
            0 => FileFormat::Iceberg,
            1 => FileFormat::Parquet,
            b => {
                return Err(CheckpointError::Internal {
                    message: format!("unknown file format tag: {b}"),
                });
            }
        };
        Ok(FileMetadata::new(format, bytes[1..].to_vec()))
    }

    async fn committed_set(&self) -> CheckpointResult<HashSet<CheckpointId>> {
        let paths = self.glob_paths(&self.committed_log_glob()).await?;
        let raw_bytes = try_join_all(paths.iter().map(|path| self.get_bytes(path))).await?;
        let mut set = HashSet::new();
        for (path, raw) in paths.iter().zip(raw_bytes) {
            let entry: CommittedLogEntry =
                serde_json::from_slice(&raw).map_err(|e| CheckpointError::Internal {
                    message: format!("failed to parse committed_log entry at {path}: {e}"),
                })?;
            for id_str in &entry.committed_checkpoint_ids {
                if let Ok(uuid) = Uuid::parse_str(id_str) {
                    set.insert(CheckpointId::from_uuid(uuid));
                }
            }
        }
        Ok(set)
    }

    fn parse_id_from_manifest_path(&self, path: &str) -> Option<CheckpointId> {
        let after_prefix = path.strip_prefix(&self.prefix)?.strip_prefix('/')?;
        let uuid_str = after_prefix.strip_suffix("/manifest.json")?;
        Uuid::parse_str(uuid_str).ok().map(CheckpointId::from_uuid)
    }

    async fn read_manifest(&self, id: CheckpointId) -> CheckpointResult<Manifest> {
        let path = self.manifest_path(id);
        let raw = self.get_bytes(&path).await?;
        serde_json::from_slice(&raw).map_err(|e| CheckpointError::Internal {
            message: format!("failed to parse manifest at {path}: {e}"),
        })
    }

    async fn write_manifest(&self, id: CheckpointId, manifest: &Manifest) -> CheckpointResult<()> {
        let path = self.manifest_path(id);
        let raw = serde_json::to_vec(manifest).map_err(|e| CheckpointError::Internal {
            message: format!("failed to serialize manifest: {e}"),
        })?;
        self.put_bytes(&path, raw).await
    }
}

#[async_trait]
impl CheckpointStore for S3CheckpointStore {
    async fn stage_keys(&self, id: CheckpointId, keys: Series) -> CheckpointResult<()> {
        let already_staged = {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            staged.contains_key(&id)
        };

        // Avoid a network round-trip if the ID is already in the staged map.
        if !already_staged && self.get_bytes(&self.manifest_path(id)).await.is_ok() {
            return Err(CheckpointError::AlreadySealed { id });
        }

        let mut staged = self.staged.write().map_err(|e| CheckpointError::Internal {
            message: format!("staged lock poisoned: {e}"),
        })?;
        let entry = staged.entry(id).or_insert_with(StagedEntry::new);
        entry.keys.push(keys);
        Ok(())
    }

    async fn stage_files(
        &self,
        id: CheckpointId,
        files: Vec<FileMetadata>,
    ) -> CheckpointResult<()> {
        let already_staged = {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            staged.contains_key(&id)
        };

        if !already_staged && self.get_bytes(&self.manifest_path(id)).await.is_ok() {
            return Err(CheckpointError::AlreadySealed { id });
        }

        let mut staged = self.staged.write().map_err(|e| CheckpointError::Internal {
            message: format!("staged lock poisoned: {e}"),
        })?;
        let entry = staged.entry(id).or_insert_with(StagedEntry::new);
        entry.files.extend(files);
        Ok(())
    }

    async fn checkpoint(&self, id: CheckpointId) -> CheckpointResult<()> {
        // Idempotency: manifest already exists → already sealed.
        if self.get_bytes(&self.manifest_path(id)).await.is_ok() {
            return Ok(());
        }

        // Serialize while holding the read lock (no `.await` in this block).
        // The entry stays in `staged` until manifest.json is durably written —
        // a failed upload leaves the staged data intact so the caller can retry.
        let (num_key_files, num_file_files, key_ipcs, file_bins, created_at) = {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            let entry = staged
                .get(&id)
                .ok_or(CheckpointError::CheckpointNotFound { id })?;
            let key_ipcs: CheckpointResult<Vec<Vec<u8>>> =
                entry.keys.iter().map(Self::series_to_ipc).collect();
            let file_bins: CheckpointResult<Vec<Vec<u8>>> =
                entry.files.iter().map(Self::encode_file_metadata).collect();
            (
                entry.keys.len(),
                entry.files.len(),
                key_ipcs?,
                file_bins?,
                entry.created_at,
            )
        };

        // Upload keys and files concurrently, all before manifest.json.
        let key_paths: Vec<String> = (0..num_key_files).map(|i| self.key_path(id, i)).collect();
        let file_paths: Vec<String> = (0..num_file_files).map(|i| self.file_path(id, i)).collect();
        try_join_all(
            key_paths
                .iter()
                .zip(key_ipcs)
                .map(|(path, ipc)| self.put_bytes(path, ipc)),
        )
        .await?;
        try_join_all(
            file_paths
                .iter()
                .zip(file_bins)
                .map(|(path, bin)| self.put_bytes(path, bin)),
        )
        .await?;

        // manifest.json is written last — its presence is the atomic seal.
        let manifest = Manifest {
            checkpoint_id: id.as_uuid().to_string(),
            created_at: rfc3339_from(created_at),
            sealed_at: rfc3339_now(),
            num_key_files,
            num_file_files,
        };
        self.write_manifest(id, &manifest).await?;

        // Remove from staged only after manifest is durably written.
        self.staged
            .write()
            .map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?
            .remove(&id);

        Ok(())
    }

    async fn get_checkpointed_keys(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Series>>> {
        let manifest_paths = self.glob_paths(&self.manifests_glob()).await?;

        // Include both checkpointed and committed keys — all are needed for the
        // skip-on-rerun filter.
        let mut key_paths: Vec<String> = Vec::new();
        for path in &manifest_paths {
            let Some(id) = self.parse_id_from_manifest_path(path) else {
                continue;
            };
            let manifest = self.read_manifest(id).await?;
            for i in 0..manifest.num_key_files {
                key_paths.push(self.key_path(id, i));
            }
        }

        let client = self.io_client()?;
        let stream = futures::stream::iter(key_paths).then(move |path| {
            let client = client.clone();
            async move {
                let result = client
                    .single_url_get(path.clone(), None, None)
                    .await
                    .map_err(|e| CheckpointError::Internal {
                        message: format!("GET {path}: {e}"),
                    })?;
                let raw = result
                    .bytes()
                    .await
                    .map_err(|e| CheckpointError::Internal {
                        message: format!("reading {path}: {e}"),
                    })?;
                Self::ipc_to_series(&raw)
            }
        });

        Ok(Box::pin(stream))
    }

    async fn get_checkpointed_files(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<FileMetadata>>> {
        let manifest_paths = self.glob_paths(&self.manifests_glob()).await?;
        let committed = self.committed_set().await?;

        let mut file_paths: Vec<String> = Vec::new();
        for path in &manifest_paths {
            let Some(id) = self.parse_id_from_manifest_path(path) else {
                continue;
            };
            if committed.contains(&id) {
                continue; // Already committed to the catalog — files no longer needed.
            }
            let manifest = self.read_manifest(id).await?;
            for i in 0..manifest.num_file_files {
                file_paths.push(self.file_path(id, i));
            }
        }

        let client = self.io_client()?;
        let stream = futures::stream::iter(file_paths).then(move |path| {
            let client = client.clone();
            async move {
                let result = client
                    .single_url_get(path.clone(), None, None)
                    .await
                    .map_err(|e| CheckpointError::Internal {
                        message: format!("GET {path}: {e}"),
                    })?;
                let raw = result
                    .bytes()
                    .await
                    .map_err(|e| CheckpointError::Internal {
                        message: format!("reading {path}: {e}"),
                    })?;
                Self::decode_file_metadata(&raw)
            }
        });

        Ok(Box::pin(stream))
    }

    async fn get_checkpoint(&self, id: CheckpointId) -> CheckpointResult<Checkpoint> {
        {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            if let Some(entry) = staged.get(&id) {
                return Ok(Checkpoint::new(
                    id,
                    CheckpointStatus::Staged,
                    entry.created_at,
                    None,
                    None,
                ));
            }
        }

        let manifest = self
            .read_manifest(id)
            .await
            .map_err(|_| CheckpointError::CheckpointNotFound { id })?;

        let committed = self.committed_set().await?;
        let status = if committed.contains(&id) {
            CheckpointStatus::Committed
        } else {
            CheckpointStatus::Checkpointed
        };

        Ok(Checkpoint::new(
            id,
            status,
            system_time_from_rfc3339(&manifest.created_at),
            Some(system_time_from_rfc3339(&manifest.sealed_at)),
            None, // committed_at not stored; committed_log entries carry no timestamp
        ))
    }

    async fn list_checkpoints(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Checkpoint>>> {
        let mut checkpoints: Vec<Checkpoint> = Vec::new();

        {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            for (&id, entry) in staged.iter() {
                checkpoints.push(Checkpoint::new(
                    id,
                    CheckpointStatus::Staged,
                    entry.created_at,
                    None,
                    None,
                ));
            }
        }

        let manifest_paths = self.glob_paths(&self.manifests_glob()).await?;
        let committed = self.committed_set().await?;

        for path in &manifest_paths {
            let Some(id) = self.parse_id_from_manifest_path(path) else {
                continue;
            };
            let manifest = match self.read_manifest(id).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let status = if committed.contains(&id) {
                CheckpointStatus::Committed
            } else {
                CheckpointStatus::Checkpointed
            };
            checkpoints.push(Checkpoint::new(
                id,
                status,
                system_time_from_rfc3339(&manifest.created_at),
                Some(system_time_from_rfc3339(&manifest.sealed_at)),
                None,
            ));
        }

        Ok(Box::pin(futures::stream::iter(
            checkpoints.into_iter().map(Ok),
        )))
    }

    async fn mark_committed(&self, ids: &[CheckpointId]) -> CheckpointResult<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let already_committed = self.committed_set().await?;

        for &id in ids {
            if already_committed.contains(&id) {
                continue;
            }
            if self.get_bytes(&self.manifest_path(id)).await.is_err() {
                // No manifest → staged or unknown.
                let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                    message: format!("staged lock poisoned: {e}"),
                })?;
                return if staged.contains_key(&id) {
                    Err(CheckpointError::NotCheckpointed { id })
                } else {
                    Err(CheckpointError::CheckpointNotFound { id })
                };
            }
        }

        let new_ids: Vec<String> = ids
            .iter()
            .filter(|&&id| !already_committed.contains(&id))
            .map(|id| id.as_uuid().to_string())
            .collect();

        if new_ids.is_empty() {
            return Ok(());
        }

        // Single atomic PutObject: crash before → nothing recorded; crash after → all IDs marked.
        let entry = CommittedLogEntry {
            committed_checkpoint_ids: new_ids,
        };
        let raw = serde_json::to_vec(&entry).map_err(|e| CheckpointError::Internal {
            message: format!("failed to serialize committed_log entry: {e}"),
        })?;
        self.put_bytes(&self.new_committed_log_path(), raw).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_encode_decode_file_metadata_roundtrip() {
        for (format, tag) in [(FileFormat::Iceberg, 0u8), (FileFormat::Parquet, 1u8)] {
            let original = FileMetadata::new(format, vec![1, 2, 3, 4]);
            let encoded = S3CheckpointStore::encode_file_metadata(&original).unwrap();
            assert_eq!(encoded[0], tag);
            assert_eq!(&encoded[1..], &[1, 2, 3, 4]);
            let decoded = S3CheckpointStore::decode_file_metadata(&encoded).unwrap();
            assert_eq!(decoded, original);
        }
    }

    #[test]
    fn test_encode_file_metadata_empty_payload() {
        let original = FileMetadata::new(FileFormat::Iceberg, vec![]);
        let encoded = S3CheckpointStore::encode_file_metadata(&original).unwrap();
        assert_eq!(encoded, vec![0u8]);
        let decoded = S3CheckpointStore::decode_file_metadata(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_file_metadata_unknown_tag() {
        let err = S3CheckpointStore::decode_file_metadata(&[42u8, 1, 2, 3]).unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }

    #[test]
    fn test_decode_file_metadata_empty_bytes() {
        let err = S3CheckpointStore::decode_file_metadata(&[]).unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }

    #[test]
    fn test_parse_id_from_manifest_path() {
        let store = S3CheckpointStore::new("s3://my-bucket/my-job", Arc::new(IOConfig::default()));
        let id = CheckpointId::generate();
        let path = store.manifest_path(id);
        let parsed = store.parse_id_from_manifest_path(&path).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_parse_id_from_manifest_path_invalid() {
        let store = S3CheckpointStore::new("s3://my-bucket/my-job", Arc::new(IOConfig::default()));
        assert!(
            store
                .parse_id_from_manifest_path("s3://other-bucket/not-a-uuid/manifest.json")
                .is_none()
        );
        assert!(
            store
                .parse_id_from_manifest_path("s3://my-bucket/my-job/not-a-uuid/manifest.json")
                .is_none()
        );
    }

    #[test]
    fn test_path_helpers() {
        let store = S3CheckpointStore::new("s3://bucket/prefix/", Arc::new(IOConfig::default()));
        // Trailing slash is stripped.
        assert!(store.prefix.ends_with("prefix"));
        let id = CheckpointId::generate();
        let uuid = id.as_uuid().to_string();
        assert_eq!(
            store.key_path(id, 3),
            format!("s3://bucket/prefix/{uuid}/keys/0003.ipc")
        );
        assert_eq!(
            store.file_path(id, 0),
            format!("s3://bucket/prefix/{uuid}/files/0000.bin")
        );
        assert_eq!(
            store.manifest_path(id),
            format!("s3://bucket/prefix/{uuid}/manifest.json")
        );
        assert_eq!(store.manifests_glob(), "s3://bucket/prefix/*/manifest.json");
        assert_eq!(
            store.committed_log_glob(),
            "s3://bucket/prefix/committed_log/*.json"
        );
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let now = rfc3339_now();
        let manifest = Manifest {
            checkpoint_id: Uuid::new_v4().to_string(),
            created_at: now.clone(),
            sealed_at: now.clone(),
            num_key_files: 3,
            num_file_files: 1,
        };
        let raw = serde_json::to_vec(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded.checkpoint_id, manifest.checkpoint_id);
        assert_eq!(decoded.created_at, now);
        assert_eq!(decoded.sealed_at, now);
        assert_eq!(decoded.num_key_files, 3);
        assert_eq!(decoded.num_file_files, 1);
    }

    #[test]
    fn test_rfc3339_roundtrip() {
        let original = SystemTime::now();
        // Round-trip loses sub-second precision — expected.
        let s = rfc3339_from(original);
        let recovered = system_time_from_rfc3339(&s);
        let diff = original
            .duration_since(recovered)
            .or_else(|e| Ok::<_, ()>(e.duration()))
            .unwrap();
        assert!(diff < Duration::from_secs(1));
        assert!(s.ends_with('Z'));
        assert_eq!(s.len(), "2026-04-03T10:30:00Z".len());
    }

    #[test]
    fn test_committed_log_entry_serde_roundtrip() {
        let entry = CommittedLogEntry {
            committed_checkpoint_ids: vec![Uuid::new_v4().to_string(), Uuid::new_v4().to_string()],
        };
        let raw = serde_json::to_vec(&entry).unwrap();
        let decoded: CommittedLogEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            decoded.committed_checkpoint_ids,
            entry.committed_checkpoint_ids
        );
    }

    #[test]
    fn test_default_cleanup_policy_is_manual() {
        assert!(matches!(CleanupPolicy::default(), CleanupPolicy::Manual));
    }
}
