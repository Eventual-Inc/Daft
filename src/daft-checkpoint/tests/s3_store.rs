//! Integration tests for [`S3CheckpointStore`] using the local filesystem backend.
//!
//! These tests exercise the full S3CheckpointStore lifecycle against a real
//! (but local) object store, verifying that the keys/, files/, and manifest.json
//! layout behaves correctly.

use std::sync::Arc;

use daft_checkpoint::{
    CheckpointError, CheckpointId, CheckpointStatus, CheckpointStore, FileFormat, FileMetadata,
    impls::S3CheckpointStore,
};
use daft_core::{
    datatypes::Utf8Array,
    series::{IntoSeries, Series},
};
use daft_io::IOConfig;
use futures::TryStreamExt;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Create a temporary directory and return an `S3CheckpointStore` backed by it.
///
/// daft-io's local filesystem backend accepts paths in the format
/// `file:///absolute/path`.
fn make_store() -> (tempfile::TempDir, S3CheckpointStore) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    // Normalize to forward slashes and ensure the canonical triple-slash `file:///`
    // form on Windows, where `display()` yields `C:\Users\...` without a leading slash.
    let raw = dir.path().display().to_string().replace('\\', "/");
    let prefix = if raw.starts_with('/') {
        format!("file://{raw}")
    } else {
        format!("file:///{raw}")
    };
    let store = S3CheckpointStore::new(prefix, Arc::new(IOConfig::default())).unwrap();
    (dir, store)
}

fn keys(values: &[&str]) -> Series {
    Utf8Array::from_slice("key", values).into_series()
}

fn file(data: &[u8]) -> FileMetadata {
    FileMetadata::new(FileFormat::Iceberg, data.to_vec())
}

async fn collect_key_strings(store: &S3CheckpointStore) -> Vec<String> {
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

async fn collect_files(store: &S3CheckpointStore) -> Vec<FileMetadata> {
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

// ---------------------------------------------------------------------------
// 1. Happy path lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_lifecycle() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    // Store is empty.
    let checkpoints: Vec<_> = store
        .list_checkpoints()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(checkpoints.is_empty());

    store.stage_keys(&id, keys(&["a", "b"])).await.unwrap();
    store
        .stage_files(&id, vec![file(b"file1"), file(b"file2")])
        .await
        .unwrap();

    // Staged data is invisible.
    assert_eq!(collect_key_strings(&store).await, Vec::<String>::new());
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());

    // Seal makes data visible.
    store.checkpoint(&id).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a", "b"]);
    assert_eq!(
        collect_files(&store).await,
        vec![file(b"file1"), file(b"file2")]
    );

    // Commit hides files but keeps keys.
    store.mark_committed(&[id]).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a", "b"]);
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());
}

// ---------------------------------------------------------------------------
// 2. Multiple checkpoints, incremental staging, partial commit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_checkpoints_and_partial_commit() {
    let (_dir, store) = make_store();
    let id1 = CheckpointId::generate(1);
    let id2 = CheckpointId::generate(2);

    // Checkpoint 1
    store.stage_keys(&id1, keys(&["a"])).await.unwrap();
    store.stage_files(&id1, vec![file(b"f1")]).await.unwrap();
    store.checkpoint(&id1).await.unwrap();

    // Checkpoint 2: multiple stage_keys calls → multiple keys/*.parquet files
    store.stage_keys(&id2, keys(&["b"])).await.unwrap();
    store.stage_keys(&id2, keys(&["c", "d"])).await.unwrap();
    store.stage_files(&id2, vec![file(b"f2")]).await.unwrap();
    store.stage_files(&id2, vec![file(b"f3")]).await.unwrap();
    store.checkpoint(&id2).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, vec!["a", "b", "c", "d"]);
    assert_eq!(
        collect_files(&store).await,
        vec![file(b"f1"), file(b"f2"), file(b"f3")]
    );

    // Commit only id1
    store.mark_committed(&[id1]).await.unwrap();

    // All keys still visible; only id2's files remain
    assert_eq!(collect_key_strings(&store).await, vec!["a", "b", "c", "d"]);
    assert_eq!(collect_files(&store).await, vec![file(b"f2"), file(b"f3")]);
}

// ---------------------------------------------------------------------------
// 3. Idempotency: checkpoint() and mark_committed() are no-ops on repeat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    store.stage_keys(&id, keys(&["a"])).await.unwrap();
    store.stage_files(&id, vec![file(b"f1")]).await.unwrap();

    // Double checkpoint()
    store.checkpoint(&id).await.unwrap();
    store.checkpoint(&id).await.unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a"]);
    assert_eq!(collect_files(&store).await, vec![file(b"f1")]);

    // Double mark_committed()
    store
        .mark_committed(std::slice::from_ref(&id))
        .await
        .unwrap();
    store
        .mark_committed(std::slice::from_ref(&id))
        .await
        .unwrap();
    assert_eq!(collect_key_strings(&store).await, vec!["a"]);
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());

    // checkpoint() on an already-committed ID is a no-op
    store.checkpoint(&id).await.unwrap();
}

// ---------------------------------------------------------------------------
// 4. Error paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_error_paths() {
    let (_dir, store) = make_store();

    // mark_committed() on an unknown ID
    let err = store
        .mark_committed(&[CheckpointId::generate(0)])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CheckpointError::CheckpointNotFound { .. } | CheckpointError::NotCheckpointed { .. }
    ));

    // stage_keys after checkpoint() → AlreadySealed
    let id = CheckpointId::generate(0);
    store.stage_keys(&id, keys(&["a"])).await.unwrap();
    store.checkpoint(&id).await.unwrap();

    let err = store.stage_keys(&id, keys(&["b"])).await.unwrap_err();
    assert!(matches!(err, CheckpointError::AlreadySealed { .. }));

    let err = store.stage_files(&id, vec![file(b"f")]).await.unwrap_err();
    assert!(matches!(err, CheckpointError::AlreadySealed { .. }));

    // mark_committed on staged (not yet checkpointed)
    let id2 = CheckpointId::generate(1);
    store.stage_keys(&id2, keys(&["x"])).await.unwrap();
    let err = store.mark_committed(&[id2]).await.unwrap_err();
    assert!(matches!(err, CheckpointError::NotCheckpointed { .. }));
}

// ---------------------------------------------------------------------------
// 5. Orphaned staged entries (simulating crash before checkpoint())
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_orphaned_staged_entries_invisible() {
    let (_dir, store) = make_store();

    // Orphan: staged but never checkpointed (crash simulation)
    let orphan_id = CheckpointId::generate(0);
    store
        .stage_keys(&orphan_id, keys(&["orphan"]))
        .await
        .unwrap();
    store
        .stage_files(&orphan_id, vec![file(b"orphan_file")])
        .await
        .unwrap();

    // Good: fully checkpointed
    let good_id = CheckpointId::generate(1);
    store.stage_keys(&good_id, keys(&["good"])).await.unwrap();
    store
        .stage_files(&good_id, vec![file(b"good_file")])
        .await
        .unwrap();
    store.checkpoint(&good_id).await.unwrap();

    // Only the good checkpoint's data is visible
    assert_eq!(collect_key_strings(&store).await, vec!["good"]);
    assert_eq!(collect_files(&store).await, vec![file(b"good_file")]);
}

// ---------------------------------------------------------------------------
// 6. Keys-only checkpoint (no files — valid for non-2PC sinks)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkpoint_keys_only() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    store.stage_keys(&id, keys(&["a", "b"])).await.unwrap();
    store.checkpoint(&id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, vec!["a", "b"]);
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());
}

// ---------------------------------------------------------------------------
// 7. Files-only checkpoint (no keys — e.g., recovery-only use case)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkpoint_files_only() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    store
        .stage_files(&id, vec![file(b"data1"), file(b"data2")])
        .await
        .unwrap();
    store.checkpoint(&id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, Vec::<String>::new());
    assert_eq!(
        collect_files(&store).await,
        vec![file(b"data1"), file(b"data2")]
    );
}

// ---------------------------------------------------------------------------
// 8. get_checkpoint / list_checkpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_checkpoint_and_list() {
    let (_dir, store) = make_store();
    let id1 = CheckpointId::generate(1);
    let id2 = CheckpointId::generate(2);

    // Unknown ID
    let err = store
        .get_checkpoint(&CheckpointId::generate(0))
        .await
        .unwrap_err();
    assert!(matches!(err, CheckpointError::CheckpointNotFound { .. }));

    // Staged → Checkpointed → Committed via get_checkpoint
    store.stage_keys(&id1, keys(&["a"])).await.unwrap();
    let ckpt = store.get_checkpoint(&id1).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Staged);
    assert!(ckpt.sealed_at.is_none());
    assert!(ckpt.committed_at.is_none());

    store.checkpoint(&id1).await.unwrap();
    let ckpt = store.get_checkpoint(&id1).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Checkpointed);
    assert!(ckpt.sealed_at.is_some());
    assert!(ckpt.committed_at.is_none());
    assert!(ckpt.sealed_at.unwrap() >= ckpt.created_at);

    store
        .mark_committed(std::slice::from_ref(&id1))
        .await
        .unwrap();
    let ckpt = store.get_checkpoint(&id1).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Committed);
    assert!(ckpt.committed_at.is_some());

    // list_checkpoints: mixed state
    store.stage_keys(&id2, keys(&["b"])).await.unwrap();

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

// ---------------------------------------------------------------------------
// 9. Empty inputs and edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_inputs() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    store.stage_keys(&id, keys(&[])).await.unwrap();
    store.stage_files(&id, vec![]).await.unwrap();
    store.checkpoint(&id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, Vec::<String>::new());
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());

    // Empty mark_committed is a no-op
    store.mark_committed(&[]).await.unwrap();
}

// ---------------------------------------------------------------------------
// 10. mark_committed: all IDs committed in a single concurrent batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mark_committed_batch() {
    let (_dir, store) = make_store();
    let ids: Vec<CheckpointId> = (1..=3).map(CheckpointId::generate).collect();

    for id in &ids {
        store.stage_keys(id, keys(&["k"])).await.unwrap();
        store.stage_files(id, vec![file(b"f")]).await.unwrap();
        store.checkpoint(id).await.unwrap();
    }

    // Commit all three in a single call — concurrent manifest overwrites
    store.mark_committed(&ids).await.unwrap();

    // All are now committed
    for id in &ids {
        let ckpt = store.get_checkpoint(id).await.unwrap();
        assert_eq!(ckpt.status, CheckpointStatus::Committed);
    }
    // Files are no longer returned
    assert_eq!(collect_files(&store).await, Vec::<FileMetadata>::new());
    // Keys still visible
    assert_eq!(collect_key_strings(&store).await, vec!["k", "k", "k"]);
}

// ---------------------------------------------------------------------------
// 11. mark_committed idempotent re-commit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mark_committed_idempotent_retry() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    store.stage_keys(&id, keys(&["a"])).await.unwrap();
    store.checkpoint(&id).await.unwrap();

    store
        .mark_committed(std::slice::from_ref(&id))
        .await
        .unwrap();
    // Retry: should be a no-op (no error)
    store
        .mark_committed(std::slice::from_ref(&id))
        .await
        .unwrap();

    let ckpt = store.get_checkpoint(&id).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Committed);
}

// ---------------------------------------------------------------------------
// 12. Multiple key batches → multiple parquet files
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_key_batches_become_separate_files() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    // 3 separate stage_keys calls → 3 parquet files
    store.stage_keys(&id, keys(&["a"])).await.unwrap();
    store.stage_keys(&id, keys(&["b", "c"])).await.unwrap();
    store.stage_keys(&id, keys(&["d"])).await.unwrap();
    store.checkpoint(&id).await.unwrap();

    assert_eq!(collect_key_strings(&store).await, vec!["a", "b", "c", "d"]);
}

// ---------------------------------------------------------------------------
// 13. FileMetadata with Parquet format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_parquet_file_metadata_roundtrip() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);
    let parquet_file = FileMetadata::new(FileFormat::Parquet, vec![10, 20, 30]);

    store
        .stage_files(&id, vec![parquet_file.clone()])
        .await
        .unwrap();
    store.checkpoint(&id).await.unwrap();

    let files: Vec<_> = store
        .get_checkpointed_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].format, FileFormat::Parquet);
    assert_eq!(files[0].data, vec![10, 20, 30]);
}

// ---------------------------------------------------------------------------
// 14. sealed_file_paths visibility lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sealed_file_paths_lifecycle() {
    let (_dir, store) = make_store();

    // Empty store → no paths.
    assert!(store.sealed_file_paths().await.unwrap().is_empty());

    // Staged only → still no paths (staged-only keys must be invisible so the
    // current run doesn't wrongly skip work that was never durably recorded).
    let id = CheckpointId::generate(0);
    store.stage_keys(&id, keys(&["a", "b"])).await.unwrap();
    assert!(store.sealed_file_paths().await.unwrap().is_empty());

    // Sealed → at least one path.
    store.checkpoint(&id).await.unwrap();
    assert!(!store.sealed_file_paths().await.unwrap().is_empty());

    // After mark_committed, paths remain available — keys remain visible for
    // skip-on-rerun even after the associated files are committed.
    store
        .mark_committed(std::slice::from_ref(&id))
        .await
        .unwrap();
    assert!(!store.sealed_file_paths().await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// 15. checkpoint() on empty source seals an empty manifest
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_checkpoint_empty_source_seals_empty_manifest() {
    let (_dir, store) = make_store();
    let id = CheckpointId::generate(0);

    // Never call stage_keys / stage_files. Empty-source case: a task processed
    // 0 rows after the anti-join, so nothing was staged.
    store.checkpoint(&id).await.unwrap();

    let ckpt = store.get_checkpoint(&id).await.unwrap();
    assert_eq!(ckpt.status, CheckpointStatus::Checkpointed);
    assert!(store.sealed_file_paths().await.unwrap().is_empty());

    // Idempotent retry on already-sealed empty checkpoint.
    store.checkpoint(&id).await.unwrap();
}
