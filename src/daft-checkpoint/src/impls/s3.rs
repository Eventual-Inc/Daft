use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use daft_core::series::Series;
use daft_io::{IOConfig, get_io_client};
use daft_recordbatch::RecordBatch;
use futures::{
    StreamExt,
    future::{join_all, try_join_all},
    stream::BoxStream,
};
use serde::{Deserialize, Serialize};

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

fn system_time_from_rfc3339(s: &str) -> CheckpointResult<SystemTime> {
    s.parse::<DateTime<Utc>>()
        .map(SystemTime::from)
        .map_err(|e| CheckpointError::Internal {
            message: format!("failed to parse timestamp {s:?}: {e}"),
        })
}

/// Written to `{prefix}/{id}/manifest.json` by `checkpoint()`.
///
/// Presence of this file marks the checkpoint as sealed. `mark_committed()` overwrites
/// it with `committed_at` set, transitioning the checkpoint to the `Committed` state.
#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    checkpoint_id: String,
    created_at: String,
    sealed_at: String,
    committed_at: Option<String>,
    num_key_files: usize,
    num_file_files: usize,
}

struct StagedEntry {
    num_key_files: usize,
    num_file_files: usize,
    created_at: SystemTime,
}

impl StagedEntry {
    fn new() -> Self {
        Self {
            num_key_files: 0,
            num_file_files: 0,
            created_at: SystemTime::now(),
        }
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
///       0000.bin          ← FileMetadata blob; one file per stage_files() call
///       ...
///     manifest.json       ← written by checkpoint(); presence = sealed;
///                           overwritten by mark_committed() with committed_at set
/// ```
pub struct S3CheckpointStore {
    prefix: String,
    io_config: Arc<IOConfig>,
    staged: RwLock<HashMap<String, StagedEntry>>,
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

    fn manifest_path(&self, id: &CheckpointId) -> String {
        format!("{}/{}/manifest.json", self.prefix, id)
    }

    fn key_path(&self, id: &CheckpointId, idx: usize) -> String {
        format!("{}/{}/keys/{:04}.ipc", self.prefix, id, idx)
    }

    fn file_path(&self, id: &CheckpointId, idx: usize) -> String {
        format!("{}/{}/files/{:04}.bin", self.prefix, id, idx)
    }

    fn manifests_glob(&self) -> String {
        format!("{}/*/manifest.json", self.prefix)
    }

    fn io_client(&self) -> CheckpointResult<Arc<daft_io::IOClient>> {
        get_io_client(true, self.io_config.clone()).map_err(|e| CheckpointError::Internal {
            message: format!("failed to create IO client: {e}"),
        })
    }

    async fn put_bytes(&self, path: &str, data: Vec<u8>) -> CheckpointResult<()> {
        // Tests run against a local filesystem backend (`file://`), which requires
        // parent directories to be created before writing — S3 does not.
        #[cfg(any(test, feature = "test-utils"))]
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

    fn parse_id_from_manifest_path(&self, path: &str) -> Option<CheckpointId> {
        let after_prefix = path.strip_prefix(&self.prefix)?.strip_prefix('/')?;
        let id_str = after_prefix.strip_suffix("/manifest.json")?;
        if !CheckpointId::is_valid(id_str) {
            return None;
        }
        Some(CheckpointId::from_string(id_str.to_string()))
    }

    async fn read_manifest(&self, id: &CheckpointId) -> CheckpointResult<Manifest> {
        let path = self.manifest_path(id);
        let raw = self.get_bytes(&path).await?;
        serde_json::from_slice(&raw).map_err(|e| CheckpointError::Internal {
            message: format!("failed to parse manifest at {path}: {e}"),
        })
    }

    async fn write_manifest(&self, id: &CheckpointId, manifest: &Manifest) -> CheckpointResult<()> {
        let path = self.manifest_path(id);
        let raw = serde_json::to_vec(manifest).map_err(|e| CheckpointError::Internal {
            message: format!("failed to serialize manifest: {e}"),
        })?;
        self.put_bytes(&path, raw).await
    }
}

#[async_trait]
impl CheckpointStore for S3CheckpointStore {
    async fn stage_keys(&self, id: &CheckpointId, keys: Series) -> CheckpointResult<()> {
        let key = id.to_string();
        let already_staged = {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            staged.contains_key(&key)
        };

        if !already_staged && self.get_bytes(&self.manifest_path(id)).await.is_ok() {
            return Err(CheckpointError::AlreadySealed { id: id.clone() });
        }

        let ipc = Self::series_to_ipc(&keys)?;

        // Reserve an index slot before writing to S3.
        let idx = {
            let mut staged = self.staged.write().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            let entry = staged.entry(key).or_insert_with(StagedEntry::new);
            let idx = entry.num_key_files;
            entry.num_key_files += 1;
            idx
        };

        self.put_bytes(&self.key_path(id, idx), ipc).await
    }

    async fn stage_files(
        &self,
        id: &CheckpointId,
        files: Vec<FileMetadata>,
    ) -> CheckpointResult<()> {
        let key = id.to_string();
        let already_staged = {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            staged.contains_key(&key)
        };

        if !already_staged && self.get_bytes(&self.manifest_path(id)).await.is_ok() {
            return Err(CheckpointError::AlreadySealed { id: id.clone() });
        }

        let bins: CheckpointResult<Vec<Vec<u8>>> =
            files.iter().map(Self::encode_file_metadata).collect();
        let bins = bins?;

        // Reserve index slots for all files before writing.
        let start_idx = {
            let mut staged = self.staged.write().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            let entry = staged.entry(key).or_insert_with(StagedEntry::new);
            let start = entry.num_file_files;
            entry.num_file_files += files.len();
            start
        };

        let file_paths: Vec<String> = (0..bins.len())
            .map(|i| self.file_path(id, start_idx + i))
            .collect();
        try_join_all(
            file_paths
                .iter()
                .zip(bins)
                .map(|(path, bin)| self.put_bytes(path, bin)),
        )
        .await?;

        Ok(())
    }

    async fn checkpoint(&self, id: &CheckpointId) -> CheckpointResult<()> {
        // Idempotency: manifest already exists → already sealed.
        if self.get_bytes(&self.manifest_path(id)).await.is_ok() {
            return Ok(());
        }

        // Keys and files are already in S3 — just read the counts for the manifest.
        let (num_key_files, num_file_files, created_at) = {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            let entry = staged
                .get(id.as_ref())
                .ok_or_else(|| CheckpointError::CheckpointNotFound { id: id.clone() })?;
            (entry.num_key_files, entry.num_file_files, entry.created_at)
        };

        // manifest.json is written last — its presence is the atomic seal.
        let manifest = Manifest {
            checkpoint_id: id.to_string(),
            created_at: rfc3339_from(created_at),
            sealed_at: rfc3339_now(),
            committed_at: None,
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
            .remove(id.as_ref());

        Ok(())
    }

    async fn get_checkpointed_keys(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Series>>> {
        let manifest_paths = self.glob_paths(&self.manifests_glob()).await?;
        let ids: Vec<CheckpointId> = manifest_paths
            .iter()
            .filter_map(|path| self.parse_id_from_manifest_path(path))
            .collect();

        let manifests = try_join_all(ids.iter().map(|id| self.read_manifest(id))).await?;

        // Include both checkpointed and committed keys — all are needed for the
        // skip-on-rerun filter.
        let mut key_paths: Vec<String> = Vec::new();
        for (id, manifest) in ids.iter().zip(&manifests) {
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
        let ids: Vec<CheckpointId> = manifest_paths
            .iter()
            .filter_map(|path| self.parse_id_from_manifest_path(path))
            .collect();

        let manifests = try_join_all(ids.iter().map(|id| self.read_manifest(id))).await?;

        let mut file_paths: Vec<String> = Vec::new();
        for (id, manifest) in ids.iter().zip(&manifests) {
            if manifest.committed_at.is_some() {
                continue; // Already committed to the catalog — files no longer needed.
            }
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

    async fn get_checkpoint(&self, id: &CheckpointId) -> CheckpointResult<Checkpoint> {
        {
            let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            if let Some(entry) = staged.get(id.as_ref()) {
                return Ok(Checkpoint::new(
                    id.clone(),
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
            .map_err(|_| CheckpointError::CheckpointNotFound { id: id.clone() })?;

        let status = if manifest.committed_at.is_some() {
            CheckpointStatus::Committed
        } else {
            CheckpointStatus::Checkpointed
        };

        Ok(Checkpoint::new(
            id.clone(),
            status,
            system_time_from_rfc3339(&manifest.created_at)?,
            Some(system_time_from_rfc3339(&manifest.sealed_at)?),
            manifest
                .committed_at
                .as_deref()
                .map(system_time_from_rfc3339)
                .transpose()?,
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
            for (id_str, entry) in staged.iter() {
                checkpoints.push(Checkpoint::new(
                    CheckpointId::from_string(id_str.clone()),
                    CheckpointStatus::Staged,
                    entry.created_at,
                    None,
                    None,
                ));
            }
        }

        let manifest_paths = self.glob_paths(&self.manifests_glob()).await?;
        let ids: Vec<CheckpointId> = manifest_paths
            .iter()
            .filter_map(|path| self.parse_id_from_manifest_path(path))
            .collect();

        // Read all manifests concurrently, tolerating individual failures.
        let results = join_all(ids.iter().map(|id| self.read_manifest(id))).await;
        for (id, result) in ids.iter().zip(results) {
            let manifest = match result {
                Ok(m) => m,
                Err(_) => continue,
            };
            let status = if manifest.committed_at.is_some() {
                CheckpointStatus::Committed
            } else {
                CheckpointStatus::Checkpointed
            };
            checkpoints.push(Checkpoint::new(
                id.clone(),
                status,
                system_time_from_rfc3339(&manifest.created_at).unwrap_or(SystemTime::UNIX_EPOCH),
                Some(
                    system_time_from_rfc3339(&manifest.sealed_at).unwrap_or(SystemTime::UNIX_EPOCH),
                ),
                manifest
                    .committed_at
                    .as_deref()
                    .map(|s| system_time_from_rfc3339(s).unwrap_or(SystemTime::UNIX_EPOCH)),
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

        // Read all manifests concurrently — also validates each checkpoint is sealed.
        // Use join_all (not try_join_all) so we can map errors to specific types.
        let results = join_all(ids.iter().map(|id| self.read_manifest(id))).await;

        let mut to_update: Vec<(&CheckpointId, Manifest)> = Vec::new();
        for (id, result) in ids.iter().zip(results) {
            match result {
                Ok(m) if m.committed_at.is_some() => {} // already committed, skip
                Ok(m) => to_update.push((id, m)),
                Err(_) => {
                    // Distinguish staged (not yet checkpointed) from completely unknown.
                    let staged = self.staged.read().map_err(|e| CheckpointError::Internal {
                        message: format!("staged lock poisoned: {e}"),
                    })?;
                    return if staged.contains_key(id.as_ref()) {
                        Err(CheckpointError::NotCheckpointed { id: id.clone() })
                    } else {
                        Err(CheckpointError::CheckpointNotFound { id: id.clone() })
                    };
                }
            }
        }

        if to_update.is_empty() {
            return Ok(());
        }

        let committed_at = rfc3339_now();

        // Write updated manifests concurrently — partial failure is safe to retry.
        try_join_all(to_update.iter().map(|(id, manifest)| {
            let updated = Manifest {
                committed_at: Some(committed_at.clone()),
                ..manifest.clone()
            };
            async move { self.write_manifest(id, &updated).await }
        }))
        .await?;

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
        let id = CheckpointId::generate(0);
        let path = store.manifest_path(&id);
        let parsed = store.parse_id_from_manifest_path(&path).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_parse_id_from_manifest_path_invalid() {
        let store = S3CheckpointStore::new("s3://my-bucket/my-job", Arc::new(IOConfig::default()));
        // Wrong bucket — prefix doesn't match.
        assert!(
            store
                .parse_id_from_manifest_path(
                    "s3://other-bucket/my-job/task-0-checkpoint-abc/manifest.json"
                )
                .is_none()
        );
        // Empty segment between prefix and manifest.json.
        assert!(
            store
                .parse_id_from_manifest_path("s3://my-bucket/my-job//manifest.json")
                .is_none()
        );
    }

    #[test]
    fn test_path_helpers() {
        let store = S3CheckpointStore::new("s3://bucket/prefix/", Arc::new(IOConfig::default()));
        // Trailing slash is stripped.
        assert!(store.prefix.ends_with("prefix"));
        let id = CheckpointId::generate(3);
        let id_str = id.to_string();
        assert_eq!(
            store.key_path(&id, 3),
            format!("s3://bucket/prefix/{id_str}/keys/0003.ipc")
        );
        assert_eq!(
            store.file_path(&id, 0),
            format!("s3://bucket/prefix/{id_str}/files/0000.bin")
        );
        assert_eq!(
            store.manifest_path(&id),
            format!("s3://bucket/prefix/{id_str}/manifest.json")
        );
        assert_eq!(store.manifests_glob(), "s3://bucket/prefix/*/manifest.json");
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let now = rfc3339_now();
        let manifest = Manifest {
            checkpoint_id: "task-0-checkpoint-abc".to_string(),
            created_at: now.clone(),
            sealed_at: now.clone(),
            committed_at: None,
            num_key_files: 3,
            num_file_files: 1,
        };
        let raw = serde_json::to_vec(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded.checkpoint_id, manifest.checkpoint_id);
        assert_eq!(decoded.created_at, now);
        assert_eq!(decoded.sealed_at, now);
        assert_eq!(decoded.committed_at, None);
        assert_eq!(decoded.num_key_files, 3);
        assert_eq!(decoded.num_file_files, 1);
    }

    #[test]
    fn test_manifest_committed_at_serde_roundtrip() {
        let now = rfc3339_now();
        let manifest = Manifest {
            checkpoint_id: "task-0-checkpoint-abc".to_string(),
            created_at: now.clone(),
            sealed_at: now.clone(),
            committed_at: Some(now.clone()),
            num_key_files: 1,
            num_file_files: 0,
        };
        let raw = serde_json::to_vec(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded.committed_at, Some(now));
    }

    #[test]
    fn test_rfc3339_roundtrip() {
        let original = SystemTime::now();
        // Round-trip loses sub-second precision — expected.
        let s = rfc3339_from(original);
        let recovered = system_time_from_rfc3339(&s).unwrap();
        let diff = original
            .duration_since(recovered)
            .or_else(|e| Ok::<_, ()>(e.duration()))
            .unwrap();
        assert!(diff < Duration::from_secs(1));
        assert!(s.ends_with('Z'));
        assert_eq!(s.len(), "2026-04-03T10:30:00Z".len());
    }

    #[test]
    fn test_system_time_from_rfc3339_error() {
        let err = system_time_from_rfc3339("not-a-timestamp").unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }
}
