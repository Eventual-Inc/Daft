use std::{
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::SystemTime,
};

use async_trait::async_trait;
use daft_core::series::Series;
use futures::stream::{self, BoxStream};

use crate::{
    Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, FileMetadata,
    error::{CheckpointError, CheckpointResult},
};

/// Internal state for a single checkpoint entry.
#[derive(Debug)]
struct CheckpointEntry {
    status: CheckpointStatus,
    keys: Vec<Series>,
    files: Vec<FileMetadata>,
    created_at: SystemTime,
    sealed_at: Option<SystemTime>,
    committed_at: Option<SystemTime>,
}

impl CheckpointEntry {
    fn new() -> Self {
        Self {
            status: CheckpointStatus::Staged,
            keys: Vec::new(),
            files: Vec::new(),
            created_at: SystemTime::now(),
            sealed_at: None,
            committed_at: None,
        }
    }
}

/// In-memory implementation of [`CheckpointStore`].
///
/// Backed by a `HashMap` protected by a `RwLock`. Suitable for testing.
/// Not durable across process restarts.
#[derive(Debug)]
pub struct InMemoryCheckpointStore {
    // Note: std::sync::RwLock is used intentionally. The lock is never held
    // across .await points. This is a test-only impl — tokio::sync::RwLock
    // is unnecessary overhead here.
    entries: RwLock<HashMap<CheckpointId, CheckpointEntry>>,
}

impl InMemoryCheckpointStore {
    /// Create a new empty in-memory checkpoint store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn read_entries(
        &self,
    ) -> CheckpointResult<RwLockReadGuard<'_, HashMap<CheckpointId, CheckpointEntry>>> {
        self.entries.read().map_err(|e| CheckpointError::Internal {
            message: format!("lock poisoned: {e}"),
        })
    }

    fn write_entries(
        &self,
    ) -> CheckpointResult<RwLockWriteGuard<'_, HashMap<CheckpointId, CheckpointEntry>>> {
        self.entries.write().map_err(|e| CheckpointError::Internal {
            message: format!("lock poisoned: {e}"),
        })
    }

    fn collect_visible_keys(entries: &HashMap<CheckpointId, CheckpointEntry>) -> Vec<Series> {
        entries
            .values()
            .filter(|e| {
                matches!(
                    e.status,
                    CheckpointStatus::Checkpointed | CheckpointStatus::Committed
                )
            })
            .flat_map(|e| e.keys.clone())
            .collect()
    }

    fn collect_uncommitted_files(
        entries: &HashMap<CheckpointId, CheckpointEntry>,
    ) -> Vec<FileMetadata> {
        entries
            .values()
            .filter(|e| e.status == CheckpointStatus::Checkpointed)
            .flat_map(|e| e.files.clone())
            .collect()
    }

    fn collect_all_checkpoints(
        entries: &HashMap<CheckpointId, CheckpointEntry>,
    ) -> Vec<Checkpoint> {
        entries
            .iter()
            .map(|(id, e)| {
                Checkpoint::new(
                    id.clone(),
                    e.status,
                    e.created_at,
                    e.sealed_at,
                    e.committed_at,
                )
            })
            .collect()
    }
}

impl Default for InMemoryCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn stage_keys(&self, id: &CheckpointId, keys: Series) -> CheckpointResult<()> {
        let mut entries = self.write_entries()?;

        let entry = entries
            .entry(id.clone())
            .or_insert_with(CheckpointEntry::new);

        if entry.status != CheckpointStatus::Staged {
            return Err(CheckpointError::AlreadySealed { id: id.clone() });
        }

        entry.keys.push(keys);
        Ok(())
    }

    async fn stage_files(
        &self,
        id: &CheckpointId,
        files: Vec<FileMetadata>,
    ) -> CheckpointResult<()> {
        let mut entries = self.write_entries()?;

        let entry = entries
            .entry(id.clone())
            .or_insert_with(CheckpointEntry::new);

        if entry.status != CheckpointStatus::Staged {
            return Err(CheckpointError::AlreadySealed { id: id.clone() });
        }

        entry.files.extend(files);
        Ok(())
    }

    async fn checkpoint(&self, id: &CheckpointId) -> CheckpointResult<()> {
        let mut entries = self.write_entries()?;

        let entry = entries
            .get_mut(id)
            .ok_or_else(|| CheckpointError::CheckpointNotFound { id: id.clone() })?;

        // Idempotent: no-op if already checkpointed or committed.
        if entry.status != CheckpointStatus::Staged {
            return Ok(());
        }

        entry.status = CheckpointStatus::Checkpointed;
        entry.sealed_at = Some(SystemTime::now());
        Ok(())
    }

    async fn get_checkpointed_keys(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Series>>> {
        let entries = self.read_entries()?;

        // Collect into a Vec to drop the read guard before returning the stream.
        // Both checkpointed and committed keys are visible — committed keys are
        // still needed for the checkpoint filter on re-run.
        let key_chunks = Self::collect_visible_keys(&entries);
        drop(entries);

        Ok(Box::pin(stream::iter(key_chunks.into_iter().map(Ok))))
    }

    async fn get_checkpointed_files(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<FileMetadata>>> {
        let entries = self.read_entries()?;

        // Collect into a Vec to drop the read guard before returning the stream.
        // Only checkpointed (not committed) files are returned — committed files
        // have already been written to the catalog.
        let files = Self::collect_uncommitted_files(&entries);
        drop(entries);

        Ok(Box::pin(stream::iter(files.into_iter().map(Ok))))
    }

    async fn get_checkpoint(&self, id: &CheckpointId) -> CheckpointResult<Checkpoint> {
        let entries = self.read_entries()?;

        let entry = entries
            .get(id)
            .ok_or_else(|| CheckpointError::CheckpointNotFound { id: id.clone() })?;

        Ok(Checkpoint::new(
            id.clone(),
            entry.status,
            entry.created_at,
            entry.sealed_at,
            entry.committed_at,
        ))
    }

    async fn list_checkpoints(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Checkpoint>>> {
        let entries = self.read_entries()?;

        let checkpoints = Self::collect_all_checkpoints(&entries);
        drop(entries);

        Ok(Box::pin(stream::iter(checkpoints.into_iter().map(Ok))))
    }

    async fn mark_committed(&self, ids: &[CheckpointId]) -> CheckpointResult<()> {
        let mut entries = self.write_entries()?;

        for id in ids {
            let entry = entries
                .get_mut(id)
                .ok_or_else(|| CheckpointError::CheckpointNotFound { id: id.clone() })?;

            match entry.status {
                // Already committed — idempotent no-op.
                CheckpointStatus::Committed => {}
                // Checkpointed → Committed.
                CheckpointStatus::Checkpointed => {
                    entry.status = CheckpointStatus::Committed;
                    entry.committed_at = Some(SystemTime::now());
                }
                // Staged entries haven't been sealed yet.
                CheckpointStatus::Staged => {
                    return Err(CheckpointError::NotCheckpointed { id: id.clone() });
                }
            }
        }

        Ok(())
    }
}
