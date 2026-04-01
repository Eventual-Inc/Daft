use std::sync::Arc;

use daft_checkpoint::{
    CheckpointError, CheckpointId, CheckpointStatus, CheckpointStore, CheckpointStoreRef,
    FileFormat, FileMetadata, impls::InMemoryCheckpointStore,
};
use daft_core::{
    datatypes::Utf8Array,
    series::{IntoSeries, Series},
};
use futures::TryStreamExt;

fn make_store() -> InMemoryCheckpointStore {
    InMemoryCheckpointStore::new()
}

fn keys(values: &[&str]) -> Series {
    Utf8Array::from_slice("key", values).into_series()
}

fn file(data: &[u8]) -> FileMetadata {
    FileMetadata::new(FileFormat::Iceberg, data.to_vec())
}

async fn collect_key_strings(store: &InMemoryCheckpointStore) -> Vec<String> {
    let chunks: Vec<Series> = store
        .get_checkpointed_keys()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = Vec::new();
    for chunk in &chunks {
        let utf8 = chunk.utf8().expect("expected utf8 series");
        for i in 0..utf8.len() {
            if let Some(val) = utf8.get(i) {
                result.push(val.to_string());
            }
        }
    }
    result.sort();
    result
}

async fn collect_files(store: &InMemoryCheckpointStore) -> Vec<FileMetadata> {
    let mut files: Vec<_> = store
        .get_checkpointed_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    files.sort_by(|a, b| a.data.cmp(&b.data));
    files
}

// ------------------------------------------------------------------
// 1. Happy path lifecycle
// ------------------------------------------------------------------

#[tokio::test]
async fn test_lifecycle() {
    let store = make_store();
    let id = CheckpointId::generate();

    // Empty store listing
    let checkpoints: Vec<_> = store
        .list_checkpoints()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(checkpoints.is_empty());

    store.stage_keys(id, keys(&["a", "b"])).await.unwrap();
    store
        .stage_files(id, vec![file(b"file1"), file(b"file2")])
        .await
        .unwrap();

    // Staged data is invisible
    assert_eq!(collect_key_strings(&store).await, Vec::<String>::new());
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());

    // Seal makes data visible
    store.checkpoint(id).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a", "b"]);
    assert_eq!(
        collect_files(&store).await,
        vec![file(b"file1"), file(b"file2")]
    );

    // Commit hides files but keeps keys
    store.mark_committed(&[id]).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a", "b"]);
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());
}

// ------------------------------------------------------------------
// 2. Multiple checkpoints with incremental staging and partial commit
// ------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_checkpoints_and_partial_commit() {
    let store = make_store();
    let id1 = CheckpointId::generate();
    let id2 = CheckpointId::generate();

    // Checkpoint 1: single stage call
    store.stage_keys(id1, keys(&["a"])).await.unwrap();
    store.stage_files(id1, vec![file(b"f1")]).await.unwrap();
    store.checkpoint(id1).await.unwrap();

    // Checkpoint 2: incremental staging (multiple calls)
    store.stage_keys(id2, keys(&["b"])).await.unwrap();
    store.stage_keys(id2, keys(&["c", "d"])).await.unwrap();
    store.stage_files(id2, vec![file(b"f2")]).await.unwrap();
    store.stage_files(id2, vec![file(b"f3")]).await.unwrap();
    store.checkpoint(id2).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, vec!["a", "b", "c", "d"]);
    assert_eq!(
        collect_files(&store).await,
        vec![file(b"f1"), file(b"f2"), file(b"f3")]
    );

    // Commit only checkpoint 1
    store.mark_committed(&[id1]).await.unwrap();

    // All keys still visible, only checkpoint 2's files remain
    assert_eq!(collect_key_strings(&store).await, vec!["a", "b", "c", "d"]);
    assert_eq!(collect_files(&store).await, vec![file(b"f2"), file(b"f3")]);
}

// ------------------------------------------------------------------
// 3. Idempotency: seal() and mark_committed() are no-ops on repeat
// ------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency() {
    let store = make_store();
    let id = CheckpointId::generate();

    store.stage_keys(id, keys(&["a"])).await.unwrap();
    store.stage_files(id, vec![file(b"f1")]).await.unwrap();

    // Double seal
    store.checkpoint(id).await.unwrap();
    store.checkpoint(id).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a"]);
    assert_eq!(collect_files(&store).await, vec![file(b"f1")]);

    // Double mark_committed
    store.mark_committed(&[id]).await.unwrap();
    store.mark_committed(&[id]).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a"]);
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());

    // seal() on committed is also a no-op
    store.checkpoint(id).await.unwrap();
}

// ------------------------------------------------------------------
// 4. Error paths
// ------------------------------------------------------------------

#[tokio::test]
async fn test_error_paths() {
    let store = make_store();

    // Seal unknown ID
    let err = store
        .checkpoint(CheckpointId::generate())
        .await
        .unwrap_err();
    assert!(matches!(err, CheckpointError::CheckpointNotFound { .. }));

    // mark_committed unknown ID
    let err = store
        .mark_committed(&[CheckpointId::generate()])
        .await
        .unwrap_err();
    assert!(matches!(err, CheckpointError::CheckpointNotFound { .. }));

    // Stage after seal
    let id = CheckpointId::generate();
    store.stage_keys(id, keys(&["a"])).await.unwrap();
    store.checkpoint(id).await.unwrap();

    let err = store.stage_keys(id, keys(&["b"])).await.unwrap_err();
    assert!(matches!(err, CheckpointError::AlreadySealed { .. }));

    let err = store.stage_files(id, vec![file(b"f")]).await.unwrap_err();
    assert!(matches!(err, CheckpointError::AlreadySealed { .. }));

    // Stage after commit (also AlreadySealed)
    store.mark_committed(&[id]).await.unwrap();
    let err = store.stage_keys(id, keys(&["c"])).await.unwrap_err();
    assert!(matches!(err, CheckpointError::AlreadySealed { .. }));

    // mark_committed on staged (not sealed)
    let id2 = CheckpointId::generate();
    store.stage_keys(id2, keys(&["x"])).await.unwrap();
    let err = store.mark_committed(&[id2]).await.unwrap_err();
    assert!(matches!(err, CheckpointError::NotCheckpointed { .. }));
}

// ------------------------------------------------------------------
// 5. Orphaned staged entries are invisible
// ------------------------------------------------------------------

#[tokio::test]
async fn test_orphaned_staged_entries() {
    let store = make_store();

    // Orphan: staged but never sealed (simulates crash)
    let orphan_id = CheckpointId::generate();
    store
        .stage_keys(orphan_id, keys(&["orphan"]))
        .await
        .unwrap();
    store
        .stage_files(orphan_id, vec![file(b"orphan_file")])
        .await
        .unwrap();

    // Good: completed successfully
    let good_id = CheckpointId::generate();
    store.stage_keys(good_id, keys(&["good"])).await.unwrap();
    store
        .stage_files(good_id, vec![file(b"good_file")])
        .await
        .unwrap();
    store.checkpoint(good_id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, vec!["good"]);
    assert_eq!(collect_files(&store).await, vec![file(b"good_file")]);
}

// ------------------------------------------------------------------
// 6. Keys only (no files) — valid for non-2PC sinks
// ------------------------------------------------------------------

#[tokio::test]
async fn test_checkpoint_keys_only() {
    let store = make_store();
    let id = CheckpointId::generate();

    store.stage_keys(id, keys(&["a"])).await.unwrap();
    store.checkpoint(id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, vec!["a"]);
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());
}

// ------------------------------------------------------------------
// 7. CRUD: get_checkpoint, list_checkpoints
// ------------------------------------------------------------------

#[tokio::test]
async fn test_crud() {
    let store = make_store();
    let id1 = CheckpointId::generate();
    let id2 = CheckpointId::generate();

    // get_checkpoint not found
    let err = store
        .get_checkpoint(CheckpointId::generate())
        .await
        .unwrap_err();
    assert!(matches!(err, CheckpointError::CheckpointNotFound { .. }));

    // Stage → Sealed → Committed lifecycle via get_checkpoint
    store.stage_keys(id1, keys(&["a"])).await.unwrap();
    let ckpt = store.get_checkpoint(id1).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Staged);
    assert!(ckpt.sealed_at.is_none());
    assert!(ckpt.committed_at.is_none());

    store.checkpoint(id1).await.unwrap();
    let ckpt = store.get_checkpoint(id1).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Checkpointed);
    assert!(ckpt.sealed_at.is_some());
    assert!(ckpt.committed_at.is_none());

    store.mark_committed(&[id1]).await.unwrap();
    let ckpt = store.get_checkpoint(id1).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Committed);
    assert!(ckpt.sealed_at.is_some());
    assert!(ckpt.committed_at.is_some());
    assert!(ckpt.sealed_at.unwrap() >= ckpt.created_at);
    assert!(ckpt.committed_at.unwrap() >= ckpt.sealed_at.unwrap());

    // list_checkpoints with mixed states
    store.stage_keys(id2, keys(&["b"])).await.unwrap();

    let checkpoints: Vec<_> = store
        .list_checkpoints()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(checkpoints.len(), 2);

    let statuses: std::collections::HashSet<_> = checkpoints.iter().map(|c| c.status).collect();
    assert!(statuses.contains(&CheckpointStatus::Committed));
    assert!(statuses.contains(&CheckpointStatus::Staged));
}

// ------------------------------------------------------------------
// 8. Empty inputs and edge cases
// ------------------------------------------------------------------

#[tokio::test]
async fn test_empty_inputs() {
    let store = make_store();
    let id = CheckpointId::generate();

    // Empty stage calls
    store.stage_keys(id, keys(&[])).await.unwrap();
    store.stage_files(id, vec![]).await.unwrap();
    store.checkpoint(id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, Vec::<String>::new());
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());

    // Empty mark_committed
    store.mark_committed(&[]).await.unwrap();
}

// ------------------------------------------------------------------
// 9. Retry after partial failure
// ------------------------------------------------------------------

#[tokio::test]
async fn test_retry_after_partial_failure() {
    let store = make_store();
    let good_id = CheckpointId::generate();
    let staged_id = CheckpointId::generate();

    // good_id: fully sealed
    store.stage_keys(good_id, keys(&["a"])).await.unwrap();
    store.checkpoint(good_id).await.unwrap();

    // staged_id: only staged (not sealed)
    store.stage_keys(staged_id, keys(&["b"])).await.unwrap();

    // First attempt: partial failure — good_id commits, staged_id errors
    let err = store
        .mark_committed(&[good_id, staged_id])
        .await
        .unwrap_err();
    assert!(matches!(err, CheckpointError::NotCheckpointed { .. }));

    // good_id was committed before the error
    assert_eq!(
        store.get_checkpoint(good_id).await.unwrap().status,
        CheckpointStatus::Committed
    );

    // Retry the full batch — good_id is idempotent no-op, staged_id still fails
    let err = store
        .mark_committed(&[good_id, staged_id])
        .await
        .unwrap_err();
    assert!(matches!(err, CheckpointError::NotCheckpointed { .. }));

    // good_id still committed (idempotent)
    assert_eq!(
        store.get_checkpoint(good_id).await.unwrap().status,
        CheckpointStatus::Committed
    );
}

// ------------------------------------------------------------------
// 10. Object safety: CheckpointStore works as trait object
// ------------------------------------------------------------------

#[tokio::test]
async fn test_object_safety() {
    let store: CheckpointStoreRef = Arc::new(InMemoryCheckpointStore::new());
    let id = CheckpointId::generate();

    store.stage_keys(id, keys(&["a"])).await.unwrap();
    store.checkpoint(id).await.unwrap();

    let chunks: Vec<Series> = store
        .get_checkpointed_keys()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(chunks.iter().map(|s| s.len()).sum::<usize>(), 1);
}
