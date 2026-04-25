use std::sync::Arc;

use async_trait::async_trait;
use daft_core::series::Series;
use futures::stream::BoxStream;

use crate::{Checkpoint, CheckpointId, FileMetadata, error::CheckpointResult};

/// Reference-counted checkpoint store.
pub type CheckpointStoreRef = Arc<dyn CheckpointStore>;

/// Tracks processed source rows and their produced files.
///
/// Enables skipping already-processed rows on re-run (progress tracking) and
/// knowing which files to commit after a crash (recovery).
///
/// File tracking is essential for 2PC sinks (Iceberg, Delta) where the
/// checkpoint store drives the catalog commit. For non-2PC sinks (e.g.,
/// Parquet written directly to object storage), file tracking may be unused.
///
/// # Lifecycle
///
/// Each checkpoint progresses through three states:
///
/// `staged → checkpointed → committed`
///
/// - **Staged:** Keys and files written but not yet visible to readers.
/// - **Checkpointed:** Sealed — keys and files are coupled and visible.
/// - **Committed:** Catalog commit succeeded — files no longer returned by
///   [`get_checkpointed_files`], but keys remain visible for skip-on-rerun.
///
/// 1. [`stage_keys`] and [`stage_files`] accumulate data under a
///    [`CheckpointId`]. Staged data is invisible to readers.
/// 2. [`checkpoint`] seals the checkpoint — its keys and files become visible
///    to readers as an atomic unit.
/// 3. [`mark_committed`] records that the checkpoint's files have been durably
///    committed to an external catalog.
///
/// # Consistency
///
/// Checkpoint data is append-only and immutable once sealed. Readers of
/// [`get_checkpointed_keys`] and [`get_checkpointed_files`] see a
/// monotonically growing set — new checkpoints may appear between calls,
/// but existing data never changes or disappears. No isolation between
/// reads is required.
///
/// [`get_checkpointed_keys`]: CheckpointStore::get_checkpointed_keys
/// [`get_checkpointed_files`]: CheckpointStore::get_checkpointed_files
/// [`stage_keys`]: CheckpointStore::stage_keys
/// [`stage_files`]: CheckpointStore::stage_files
/// [`checkpoint`]: CheckpointStore::checkpoint
/// [`mark_committed`]: CheckpointStore::mark_committed
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Stage source keys into a checkpoint as a columnar [`Series`]. May be
    /// called multiple times for the same [`CheckpointId`]. Staged keys are
    /// not visible to readers until [`checkpoint`](Self::checkpoint) is called.
    ///
    /// Implicitly creates a `Staged` checkpoint entry on first call for a
    /// new ID. Subsequent calls for the same ID append to the existing entry.
    ///
    /// The Series can be any Arrow-compatible type (Utf8, Int64, Struct for
    /// composite keys, etc.). Callers must stage consistent Series types
    /// (same schema) across calls for the same checkpoint.
    ///
    /// Returns [`AlreadySealed`] if the checkpoint has already been sealed.
    ///
    /// **Not idempotent** — uses append semantics. Duplicate keys are not
    /// deduplicated because keys arrive incrementally in batches; dedup would
    /// add cost for no benefit at this layer.
    ///
    /// [`AlreadySealed`]: crate::error::CheckpointError::AlreadySealed
    async fn stage_keys(&self, id: &CheckpointId, keys: Series) -> CheckpointResult<()>;

    /// Stage output file metadata into a checkpoint. May be called multiple
    /// times for the same [`CheckpointId`]. Staged files are not visible to
    /// readers until [`checkpoint`](Self::checkpoint) is called.
    ///
    /// Implicitly creates a `Staged` checkpoint entry on first call for a
    /// new ID, same as [`stage_keys`](Self::stage_keys).
    ///
    /// Returns [`AlreadySealed`] if the checkpoint has already been sealed.
    ///
    /// **Not idempotent** — uses append semantics, same as [`stage_keys`](Self::stage_keys).
    ///
    /// [`AlreadySealed`]: crate::error::CheckpointError::AlreadySealed
    async fn stage_files(
        &self,
        id: &CheckpointId,
        files: Vec<FileMetadata>,
    ) -> CheckpointResult<()>;

    /// Register a checkpoint ID for in-flight tracking without staging any
    /// data. Creates an empty `Staged` entry so that a subsequent
    /// [`checkpoint`](Self::checkpoint) call succeeds even if no keys or
    /// files were staged (e.g., when the source produces 0 rows).
    ///
    /// **Idempotent** — no-op if the ID is already staged or sealed.
    async fn register(&self, id: &CheckpointId) -> CheckpointResult<()>;

    /// Checkpoint (seal) a staged entry — couples the staged keys and files, making them
    /// visible to readers. No further staging is allowed after this call.
    ///
    /// Returns [`CheckpointNotFound`] if the ID was never staged or
    /// [`register`](Self::register)ed.
    ///
    /// **Idempotent** — no-op if the checkpoint has already been sealed.
    /// This supports retry after message loss (e.g., worker sends checkpoint,
    /// acknowledgement is lost, worker retries).
    ///
    /// [`CheckpointNotFound`]: crate::error::CheckpointError::CheckpointNotFound
    async fn checkpoint(&self, id: &CheckpointId) -> CheckpointResult<()>;

    /// Stream all checkpointed source keys (both checkpointed and committed)
    /// as columnar [`Series`] chunks. Useful for building a filter to skip
    /// already-processed inputs on re-run.
    async fn get_checkpointed_keys(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Series>>>;

    /// Stream checkpointed (but not yet committed) file metadata. These are
    /// files that have been written but not yet durably committed to a catalog.
    /// Useful for driving the catalog commit — consumers read these to know
    /// which files to commit to Iceberg, Delta, or other 2PC sinks.
    async fn get_checkpointed_files(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<FileMetadata>>>;

    /// Get metadata for a single checkpoint by ID.
    async fn get_checkpoint(&self, id: &CheckpointId) -> CheckpointResult<Checkpoint>;

    /// Stream metadata for all checkpoints in the store.
    async fn list_checkpoints(
        &self,
    ) -> CheckpointResult<BoxStream<'_, CheckpointResult<Checkpoint>>>;

    /// Mark checkpoints as committed. Committed checkpoints' keys remain
    /// visible via [`get_checkpointed_keys`](Self::get_checkpointed_keys)
    /// (to skip on re-run), but their files no longer appear in
    /// [`get_checkpointed_files`](Self::get_checkpointed_files).
    ///
    /// **Idempotent** for already-committed checkpoints (no-op). Errors if a
    /// checkpoint is still in `Staged` state (not yet sealed). This supports
    /// crash recovery: if the caller crashes after committing some IDs but
    /// before finishing, a retry succeeds without error.
    ///
    /// Partial application is possible on error — IDs processed before the
    /// failing one are committed. Since the method is idempotent, retrying
    /// the full batch after failure is safe.
    async fn mark_committed(&self, ids: &[CheckpointId]) -> CheckpointResult<()>;
}
