use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use daft_core::series::Series;
use daft_io::{IOClient, IOConfig, get_io_client, strip_file_uri_to_path};
use futures::{
    StreamExt, TryStreamExt,
    future::{join_all, try_join_all},
    lock::Mutex as AsyncMutex,
    stream::{self, BoxStream},
};
use serde::{Deserialize, Serialize};

use crate::{
    Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, FileMetadata,
    error::{CheckpointError, CheckpointResult},
    types::FileFormat,
};

fn rfc3339_from(t: SystemTime) -> String {
    DateTime::<Utc>::from(t)
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn system_time_from_rfc3339(s: &str) -> CheckpointResult<SystemTime> {
    s.parse::<DateTime<Utc>>()
        .map(SystemTime::from)
        .map_err(|e| CheckpointError::Internal {
            message: format!("failed to parse timestamp {s:?}: {e}"),
        })
}

type ManifestCache = Arc<Vec<(CheckpointId, Manifest)>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ManifestStatus {
    Checkpointed,
    Committed,
}

/// Written to `{prefix}/{id}/manifest.json` by `checkpoint()`.
///
/// Presence of this file marks the checkpoint as sealed. `mark_committed()` overwrites
/// it with `committed_at` set, transitioning the checkpoint to the `Committed` state.
#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    checkpoint_id: String,
    status: ManifestStatus,
    created_at: String,
    sealed_at: String,
    committed_at: Option<String>,
    num_key_files: usize,
    num_file_files: usize,
    /// Identifier of the execution that staged this checkpoint. Older manifests
    /// predate this field; `serde(default)` yields empty on absence.
    #[serde(default)]
    query_id: String,
}

struct StagedEntry {
    num_key_files: usize,
    num_file_files: usize,
    created_at: SystemTime,
    query_id: String,
}

impl StagedEntry {
    fn new(query_id: String) -> Self {
        Self {
            num_key_files: 0,
            num_file_files: 0,
            created_at: SystemTime::now(),
            query_id,
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
///       0000.parquet      ← one file per stage_keys() call
///       ...
///     files/
///       0000.bin          ← FileMetadata blob; one file per stage_files() call
///       ...
///     manifest.json       ← written by checkpoint(); presence = sealed;
///                           overwritten by mark_committed() with committed_at set
/// ```
pub struct S3CheckpointStore {
    prefix: String,
    client: Arc<IOClient>,
    staged: Mutex<HashMap<CheckpointId, StagedEntry>>,
    /// Cached result of the last `sealed_manifests()` S3 fetch.
    /// Invalidated by `checkpoint()` and `mark_committed()`.
    manifest_cache: AsyncMutex<Option<ManifestCache>>,
}

impl S3CheckpointStore {
    /// Create a new checkpoint store rooted at `prefix` (trailing slashes stripped).
    pub fn new(prefix: impl Into<String>, io_config: Arc<IOConfig>) -> CheckpointResult<Self> {
        let client = get_io_client(true, io_config).map_err(|e| CheckpointError::Internal {
            message: format!("failed to create IO client: {e}"),
        })?;
        Ok(Self {
            prefix: prefix.into().trim_end_matches('/').to_string(),
            client,
            staged: Mutex::new(HashMap::new()),
            manifest_cache: AsyncMutex::new(None),
        })
    }

    fn manifest_path(&self, id: &CheckpointId) -> String {
        format!("{}/{}/manifest.json", self.prefix, id)
    }

    fn key_path(&self, id: &CheckpointId, idx: usize) -> String {
        format!("{}/{}/keys/{:04}.parquet", self.prefix, id, idx)
    }

    fn file_path(&self, id: &CheckpointId, idx: usize) -> String {
        format!("{}/{}/files/{:04}.bin", self.prefix, id, idx)
    }

    fn manifests_glob(&self) -> String {
        format!("{}/*/manifest.json", self.prefix)
    }

    async fn put_bytes(&self, path: &str, data: Vec<u8>) -> CheckpointResult<()> {
        // Local filesystem backend (`file://`) requires parent directories
        // to exist before writing — S3 does not. Use `strip_file_uri_to_path`
        // (rather than a hand-rolled `strip_prefix("file://")`) so the
        // Windows-canonical `file:///C:/Users/...` form has its leading slash
        // before the drive letter stripped — otherwise `std::path::Path` fails
        // with os error 123 on Windows.
        if let Some(local) = strip_file_uri_to_path(path)
            && let Some(parent) = std::path::Path::new(local).parent()
        {
            std::fs::create_dir_all(parent).map_err(|e| CheckpointError::Internal {
                message: format!("failed to create directory {}: {e}", parent.display()),
            })?;
        }
        self.client
            .single_url_put(path, Bytes::from(data), None)
            .await
            .map_err(|e| CheckpointError::Internal {
                message: format!("PUT {path} failed: {e}"),
            })
    }

    /// Returns empty vec if the prefix doesn't exist yet. Propagates other errors
    /// (permission failures, network errors, malformed URLs).
    async fn glob_paths(&self, pattern: &str) -> CheckpointResult<Vec<String>> {
        match self
            .client
            .as_ref()
            .glob(pattern.to_string(), None, None, None, None, None)
            .await
        {
            Ok(stream) => {
                let items: Vec<CheckpointResult<String>> = stream
                    .map(|r| {
                        r.map(|fm| fm.filepath)
                            .map_err(|e| CheckpointError::Internal {
                                message: format!("glob {pattern} entry failed: {e}"),
                            })
                    })
                    .collect()
                    .await;
                items.into_iter().collect()
            }
            Err(daft_io::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(CheckpointError::Internal {
                message: format!("glob {pattern} failed: {e}"),
            }),
        }
    }

    /// Packs all `files` into a single blob written by one `stage_files()` call.
    ///
    /// Format: `[count: u32 LE] ([fmt_len: u32 LE] [fmt_json: UTF-8] [data_len: u32 LE] [data...]) * count`
    /// `fmt_json` is the serde_json encoding of `FileFormat` (e.g. `"iceberg"`).
    /// New `FileFormat` variants are handled automatically via serde — no manual tag registry.
    fn encode_file_metadata(files: &[FileMetadata]) -> CheckpointResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&(files.len() as u32).to_le_bytes());
        for f in files {
            let fmt_json =
                serde_json::to_vec(&f.format).map_err(|e| CheckpointError::Internal {
                    message: format!("failed to serialize FileFormat: {e}"),
                })?;
            out.extend_from_slice(&(fmt_json.len() as u32).to_le_bytes());
            out.extend_from_slice(&fmt_json);
            out.extend_from_slice(&(f.data.len() as u32).to_le_bytes());
            out.extend_from_slice(&f.data);
        }
        Ok(out)
    }

    fn decode_file_metadata(bytes: &[u8]) -> CheckpointResult<Vec<FileMetadata>> {
        if bytes.len() < 4 {
            return Err(CheckpointError::Internal {
                message: "file metadata blob too short".to_string(),
            });
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > bytes.len() {
                return Err(CheckpointError::Internal {
                    message: "file metadata blob truncated at format length".to_string(),
                });
            }
            let fmt_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + fmt_len > bytes.len() {
                return Err(CheckpointError::Internal {
                    message: "file metadata blob truncated at format bytes".to_string(),
                });
            }
            let format: FileFormat =
                serde_json::from_slice(&bytes[pos..pos + fmt_len]).map_err(|e| {
                    CheckpointError::Internal {
                        message: format!("failed to deserialize FileFormat: {e}"),
                    }
                })?;
            pos += fmt_len;
            if pos + 4 > bytes.len() {
                return Err(CheckpointError::Internal {
                    message: "file metadata blob truncated at data length".to_string(),
                });
            }
            let data_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + data_len > bytes.len() {
                return Err(CheckpointError::Internal {
                    message: "file metadata data truncated".to_string(),
                });
            }
            result.push(FileMetadata::new(
                format,
                bytes[pos..pos + data_len].to_vec(),
            ));
            pos += data_len;
        }
        Ok(result)
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
        let result = self
            .client
            .single_url_get(path.clone(), None, None)
            .await
            .map_err(|e| match e {
                daft_io::Error::NotFound { .. } => {
                    CheckpointError::CheckpointNotFound { id: id.clone() }
                }
                e => CheckpointError::Internal {
                    message: format!("GET {path} failed: {e}"),
                },
            })?;
        let raw = result
            .bytes()
            .await
            .map_err(|e| CheckpointError::Internal {
                message: format!("reading bytes from {path} failed: {e}"),
            })?;
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

    /// Ensures the checkpoint is not yet sealed, then atomically reserves index
    /// slots by calling `update` inside the staged lock.
    ///
    /// Returns `AlreadySealed` if `manifest.json` already exists for this ID
    /// and the ID is not tracked in the in-memory staged map (i.e., this
    /// process didn't stage it).
    async fn reserve_slots(
        &self,
        id: &CheckpointId,
        query_id: &str,
        update: impl FnOnce(&mut StagedEntry) -> usize,
    ) -> CheckpointResult<usize> {
        {
            let mut staged = self.staged.lock().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            if let Some(entry) = staged.get_mut(id) {
                return Ok(update(entry));
            }
        }

        // First call for this ID — check S3 to catch post-restart misuse.
        match self.read_manifest(id).await {
            Ok(_) => return Err(CheckpointError::AlreadySealed { id: id.clone() }),
            Err(CheckpointError::CheckpointNotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        let mut staged = self.staged.lock().map_err(|e| CheckpointError::Internal {
            message: format!("staged lock poisoned: {e}"),
        })?;
        let entry = staged
            .entry(id.clone())
            .or_insert_with(|| StagedEntry::new(query_id.to_string()));
        Ok(update(entry))
    }

    async fn invalidate_manifest_cache(&self) {
        *self.manifest_cache.lock().await = None;
    }

    /// Returns all sealed manifests (checkpointed + committed) as `(id, manifest)` pairs.
    ///
    /// Results are cached until `checkpoint()` or `mark_committed()` writes to S3.
    async fn sealed_manifests(&self) -> CheckpointResult<Arc<Vec<(CheckpointId, Manifest)>>> {
        // Hold the lock for the entire fetch so concurrent callers wait rather than
        // issuing duplicate S3 requests (stampede prevention).
        let mut cache = self.manifest_cache.lock().await;
        if let Some(cached) = cache.as_ref() {
            return Ok(Arc::clone(cached));
        }

        let manifest_paths = self.glob_paths(&self.manifests_glob()).await?;
        let ids: Vec<CheckpointId> = manifest_paths
            .iter()
            .filter_map(|path| self.parse_id_from_manifest_path(path))
            .collect();

        // A manifest that was listed but returns NotFound on GET means external
        // deletion or a code bug — neither is safe to skip silently. Fail loudly so
        // the caller can surface the inconsistency rather than hiding it.
        //
        // Pre-compute owned path Strings so the stream closure captures no borrows,
        // which lets `buffered` run up to 64 GETs concurrently without HRTB issues.
        // `buffered` (vs `buffer_unordered`) preserves the id↔manifest order needed
        // for the zip below.
        let paths: Vec<String> = ids.iter().map(|id| self.manifest_path(id)).collect();
        let client = Arc::clone(&self.client);
        let manifests: Vec<Manifest> = futures::stream::iter(paths)
            .map(|path| {
                let client = Arc::clone(&client);
                async move {
                    let result = client
                        .single_url_get(path.clone(), None, None)
                        .await
                        .map_err(|e| CheckpointError::Internal {
                            message: format!("GET {path} failed: {e}"),
                        })?;
                    let raw = result
                        .bytes()
                        .await
                        .map_err(|e| CheckpointError::Internal {
                            message: format!("reading bytes from {path} failed: {e}"),
                        })?;
                    serde_json::from_slice::<Manifest>(&raw).map_err(|e| {
                        CheckpointError::Internal {
                            message: format!("failed to parse manifest at {path}: {e}"),
                        }
                    })
                }
            })
            .buffered(64)
            .try_collect()
            .await?;
        let result = Arc::new(ids.into_iter().zip(manifests).collect::<Vec<_>>());
        *cache = Some(Arc::clone(&result));
        Ok(result)
    }

    fn fetch_paths<T: Send + 'static>(
        client: Arc<IOClient>,
        paths: Vec<String>,
        decode: impl Fn(&[u8]) -> CheckpointResult<T> + Send + Sync + 'static,
    ) -> BoxStream<'static, CheckpointResult<T>> {
        let decode = Arc::new(decode);
        Box::pin(
            futures::stream::iter(paths)
                .map(move |path| {
                    let client = Arc::clone(&client);
                    let decode = Arc::clone(&decode);
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
                        decode(&raw)
                    }
                })
                .buffer_unordered(16),
        )
    }

    /// Enumerate paths of sealed (checkpointed or committed) key parquet files.
    pub async fn sealed_file_paths(&self) -> CheckpointResult<Vec<String>> {
        let sealed = self.sealed_manifests().await?;
        let mut paths: Vec<String> = Vec::new();
        for (id, manifest) in sealed.iter() {
            for i in 0..manifest.num_key_files {
                paths.push(self.key_path(id, i));
            }
        }
        Ok(paths)
    }
}

#[async_trait]
impl CheckpointStore for S3CheckpointStore {
    async fn stage_keys(
        &self,
        id: &CheckpointId,
        query_id: &str,
        keys: Series,
    ) -> CheckpointResult<()> {
        let parquet_bytes = super::keys_codec::write_series_as_parquet(&keys)?;
        let idx = self
            .reserve_slots(id, query_id, |e| {
                let i = e.num_key_files;
                e.num_key_files += 1;
                i
            })
            .await?;
        self.put_bytes(&self.key_path(id, idx), parquet_bytes).await
    }

    async fn stage_files(
        &self,
        id: &CheckpointId,
        query_id: &str,
        files: Vec<FileMetadata>,
    ) -> CheckpointResult<()> {
        if files.is_empty() {
            return Ok(());
        }
        let blob = Self::encode_file_metadata(&files)?;
        let idx = self
            .reserve_slots(id, query_id, |e| {
                let i = e.num_file_files;
                e.num_file_files += 1;
                i
            })
            .await?;
        self.put_bytes(&self.file_path(id, idx), blob).await
    }

    async fn checkpoint(&self, id: &CheckpointId) -> CheckpointResult<()> {
        // If staged by this process, reserve_slots already verified no manifest exists —
        // skip the S3 GET. Otherwise (e.g. post-restart retry), read the manifest.
        let staged_data = {
            let staged = self.staged.lock().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            staged.get(id).map(|e| {
                (
                    e.num_key_files,
                    e.num_file_files,
                    e.created_at,
                    e.query_id.clone(),
                )
            })
        }; // lock released here, before any await

        let (num_key_files, num_file_files, created_at, query_id) = match staged_data {
            Some(data) => data,
            None => {
                return match self.read_manifest(id).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                };
            }
        };

        // manifest.json is written last — its presence is the atomic seal.
        let manifest = Manifest {
            checkpoint_id: id.to_string(),
            status: ManifestStatus::Checkpointed,
            created_at: rfc3339_from(created_at),
            sealed_at: rfc3339_from(SystemTime::now()),
            committed_at: None,
            num_key_files,
            num_file_files,
            query_id,
        };
        self.write_manifest(id, &manifest).await?;
        self.invalidate_manifest_cache().await;

        // Remove from staged only after manifest is durably written.
        self.staged
            .lock()
            .map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?
            .remove(id);

        Ok(())
    }

    async fn get_checkpointed_keys(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Series>>> {
        // Include both checkpointed and committed keys — all are needed for the
        // skip-on-rerun filter.
        let mut key_paths: Vec<String> = Vec::new();
        for (id, manifest) in self.sealed_manifests().await?.iter() {
            for i in 0..manifest.num_key_files {
                key_paths.push(self.key_path(id, i));
            }
        }

        Ok(Self::fetch_paths(
            Arc::clone(&self.client),
            key_paths,
            super::keys_codec::read_series_from_parquet,
        ))
    }

    async fn get_checkpointed_files(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<FileMetadata>>> {
        let mut file_paths: Vec<String> = Vec::new();
        for (id, manifest) in self.sealed_manifests().await?.iter() {
            if manifest.status == ManifestStatus::Committed {
                continue; // Already committed to the catalog — files no longer needed.
            }
            for i in 0..manifest.num_file_files {
                file_paths.push(self.file_path(id, i));
            }
        }

        // Each .bin holds a batch of FileMetadata items (one blob per stage_files() call).
        let batch_stream = Self::fetch_paths(Arc::clone(&self.client), file_paths, |raw| {
            Self::decode_file_metadata(raw)
        });
        Ok(Box::pin(batch_stream.flat_map(|result| {
            stream::iter(match result {
                Ok(batch) => batch.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
        })))
    }

    async fn get_checkpoint(&self, id: &CheckpointId) -> CheckpointResult<Checkpoint> {
        {
            let staged = self.staged.lock().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            if let Some(entry) = staged.get(id) {
                return Ok(Checkpoint::new(
                    id.clone(),
                    CheckpointStatus::Staged,
                    Arc::from(entry.query_id.as_str()),
                    entry.created_at,
                    None,
                    None,
                ));
            }
        }

        let manifest = self.read_manifest(id).await?;

        let status = if manifest.status == ManifestStatus::Committed {
            CheckpointStatus::Committed
        } else {
            CheckpointStatus::Checkpointed
        };

        Ok(Checkpoint::new(
            id.clone(),
            status,
            Arc::from(manifest.query_id.as_str()),
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
        let sealed = self.sealed_manifests().await?;
        let sealed_ids: std::collections::HashSet<&CheckpointId> =
            sealed.iter().map(|(id, _)| id).collect();

        let mut checkpoints: Vec<Checkpoint> = Vec::new();

        // Staged entries — skip any id that already has a manifest on S3, as the
        // sealed state is authoritative and takes precedence over the in-memory entry.
        {
            let staged = self.staged.lock().map_err(|e| CheckpointError::Internal {
                message: format!("staged lock poisoned: {e}"),
            })?;
            for (id, entry) in staged.iter() {
                if sealed_ids.contains(id) {
                    continue;
                }
                checkpoints.push(Checkpoint::new(
                    id.clone(),
                    CheckpointStatus::Staged,
                    Arc::from(entry.query_id.as_str()),
                    entry.created_at,
                    None,
                    None,
                ));
            }
        }

        for (id, manifest) in sealed.iter() {
            let status = if manifest.status == ManifestStatus::Committed {
                CheckpointStatus::Committed
            } else {
                CheckpointStatus::Checkpointed
            };
            checkpoints.push(Checkpoint::new(
                id.clone(),
                status,
                Arc::from(manifest.query_id.as_str()),
                system_time_from_rfc3339(&manifest.created_at)?,
                Some(system_time_from_rfc3339(&manifest.sealed_at)?),
                manifest
                    .committed_at
                    .as_deref()
                    .map(system_time_from_rfc3339)
                    .transpose()?,
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
                Ok(m) if m.status == ManifestStatus::Committed => {}
                Ok(m) => to_update.push((id, m)),
                Err(CheckpointError::CheckpointNotFound { .. }) => {
                    // Distinguish staged (not yet checkpointed) from completely unknown.
                    let staged = self.staged.lock().map_err(|e| CheckpointError::Internal {
                        message: format!("staged lock poisoned: {e}"),
                    })?;
                    return if staged.contains_key(id) {
                        Err(CheckpointError::NotCheckpointed { id: id.clone() })
                    } else {
                        Err(CheckpointError::CheckpointNotFound { id: id.clone() })
                    };
                }
                Err(e) => return Err(e),
            }
        }

        if to_update.is_empty() {
            return Ok(());
        }

        let committed_at = rfc3339_from(SystemTime::now());

        // Partial failure is safe — each write is idempotent, caller can retry.
        try_join_all(to_update.iter().map(|(id, manifest)| {
            let updated = Manifest {
                status: ManifestStatus::Committed,
                committed_at: Some(committed_at.clone()),
                ..manifest.clone()
            };
            async move { self.write_manifest(id, &updated).await }
        }))
        .await?;
        self.invalidate_manifest_cache().await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_encode_decode_file_metadata_roundtrip() {
        // Mixed formats in one batch — one blob per stage_files() call.
        let batch = vec![
            FileMetadata::new(FileFormat::Iceberg, vec![1, 2, 3, 4]),
            FileMetadata::new(FileFormat::Parquet, vec![5, 6]),
            FileMetadata::new(FileFormat::Iceberg, vec![]),
        ];
        let encoded = S3CheckpointStore::encode_file_metadata(&batch).unwrap();
        let decoded = S3CheckpointStore::decode_file_metadata(&encoded).unwrap();
        assert_eq!(decoded, batch);
    }

    #[test]
    fn test_encode_decode_file_metadata_single_item() {
        for format in [FileFormat::Iceberg, FileFormat::Parquet] {
            let batch = vec![FileMetadata::new(format, vec![0xde, 0xad])];
            let encoded = S3CheckpointStore::encode_file_metadata(&batch).unwrap();
            let decoded = S3CheckpointStore::decode_file_metadata(&encoded).unwrap();
            assert_eq!(decoded, batch);
        }
    }

    #[test]
    fn test_encode_decode_file_metadata_empty_batch() {
        let encoded = S3CheckpointStore::encode_file_metadata(&[]).unwrap();
        // Just the count field — 4 zero bytes.
        assert_eq!(encoded, vec![0, 0, 0, 0]);
        let decoded = S3CheckpointStore::decode_file_metadata(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_decode_file_metadata_unknown_format() {
        // count=1, fmt_len=9, fmt_json=`"unknown"`, data_len=0
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        let fmt = b"\"unknown\"";
        bytes.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
        bytes.extend_from_slice(fmt);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let err = S3CheckpointStore::decode_file_metadata(&bytes).unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }

    #[test]
    fn test_decode_file_metadata_too_short() {
        let err = S3CheckpointStore::decode_file_metadata(&[0, 0, 0]).unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }

    #[test]
    fn test_decode_file_metadata_truncated_format() {
        // count=1, fmt_len=20 but only 4 bytes follow
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&20u32.to_le_bytes());
        bytes.extend_from_slice(b"abcd");
        let err = S3CheckpointStore::decode_file_metadata(&bytes).unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }

    #[test]
    fn test_parse_id_from_manifest_path() {
        let store =
            S3CheckpointStore::new("s3://my-bucket/my-job", Arc::new(IOConfig::default())).unwrap();
        let id = CheckpointId::generate(0);
        let path = store.manifest_path(&id);
        let parsed = store.parse_id_from_manifest_path(&path).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_parse_id_from_manifest_path_invalid() {
        let store =
            S3CheckpointStore::new("s3://my-bucket/my-job", Arc::new(IOConfig::default())).unwrap();
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
        let store =
            S3CheckpointStore::new("s3://bucket/prefix/", Arc::new(IOConfig::default())).unwrap();
        assert!(store.prefix.ends_with("prefix"));
        let id = CheckpointId::generate(3);
        let id_str = id.to_string();
        assert_eq!(
            store.key_path(&id, 3),
            format!("s3://bucket/prefix/{id_str}/keys/0003.parquet")
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
        let now = rfc3339_from(SystemTime::now());
        let manifest = Manifest {
            checkpoint_id: "task-0-checkpoint-abc".to_string(),
            status: ManifestStatus::Checkpointed,
            created_at: now.clone(),
            sealed_at: now.clone(),
            committed_at: None,
            num_key_files: 3,
            num_file_files: 1,
            query_id: "eager-phoenix-a4f821".to_string(),
        };
        let raw = serde_json::to_vec(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded.checkpoint_id, manifest.checkpoint_id);
        assert_eq!(decoded.status, ManifestStatus::Checkpointed);
        assert_eq!(decoded.created_at, now);
        assert_eq!(decoded.sealed_at, now);
        assert_eq!(decoded.committed_at, None);
        assert_eq!(decoded.num_key_files, 3);
        assert_eq!(decoded.num_file_files, 1);
        assert_eq!(decoded.query_id, "eager-phoenix-a4f821");
    }

    #[test]
    fn test_manifest_committed_at_serde_roundtrip() {
        let now = rfc3339_from(SystemTime::now());
        let manifest = Manifest {
            checkpoint_id: "task-0-checkpoint-abc".to_string(),
            status: ManifestStatus::Committed,
            created_at: now.clone(),
            sealed_at: now.clone(),
            committed_at: Some(now.clone()),
            num_key_files: 1,
            num_file_files: 0,
            query_id: "swift-otter-1c2b3a".to_string(),
        };
        let raw = serde_json::to_vec(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded.status, ManifestStatus::Committed);
        assert_eq!(decoded.committed_at, Some(now));
    }

    #[test]
    fn test_manifest_query_id_default_on_old_payload() {
        let now = rfc3339_from(SystemTime::now());
        // Synthetic "old" manifest without the query_id field.
        let raw = serde_json::json!({
            "checkpoint_id": "task-0-checkpoint-abc",
            "status": "checkpointed",
            "created_at": &now,
            "sealed_at": &now,
            "committed_at": null,
            "num_key_files": 0,
            "num_file_files": 0,
        });
        let decoded: Manifest = serde_json::from_value(raw).unwrap();
        assert_eq!(decoded.query_id, "");
    }

    #[test]
    fn test_rfc3339_roundtrip() {
        let original = SystemTime::now();
        let s = rfc3339_from(original);
        let recovered = system_time_from_rfc3339(&s).unwrap();
        let diff = original
            .duration_since(recovered)
            .or_else(|e| Ok::<_, ()>(e.duration()))
            .unwrap();
        assert!(diff < Duration::from_millis(1));
        assert!(s.ends_with('Z'));
        assert_eq!(s.len(), "2026-04-03T10:30:00.000Z".len());
    }

    #[test]
    fn test_system_time_from_rfc3339_error() {
        let err = system_time_from_rfc3339("not-a-timestamp").unwrap_err();
        assert!(matches!(err, CheckpointError::Internal { .. }));
    }
}
