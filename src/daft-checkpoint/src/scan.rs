//! [`ScanOperator`] for reading sealed checkpoint keys from file-backed stores.

use std::sync::Arc;

use common_checkpoint_config::CheckpointStoreConfig;
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_scan::{
    ChunkSpec, FileFormatConfig, ParquetSourceConfig, PartitionField, Pushdowns, ScanOperator,
    ScanSource, ScanSourceKind, ScanTask, ScanTaskRef, SourceConfig, storage_config::StorageConfig,
};
use daft_schema::schema::SchemaRef;

/// [`ScanOperator`] for file-backed checkpoint stores.
///
/// Non-file backends (e.g. Postgres) would need a different scan operator.
#[derive(Debug)]
pub struct BlobStoreCheckpointedKeysScanOperator {
    config: CheckpointStoreConfig,
    schema: SchemaRef,
}

impl BlobStoreCheckpointedKeysScanOperator {
    #[must_use]
    pub fn new(config: CheckpointStoreConfig, schema: SchemaRef) -> Self {
        Self { config, schema }
    }
}

impl ScanOperator for BlobStoreCheckpointedKeysScanOperator {
    fn name(&self) -> &'static str {
        "BlobStoreCheckpointedKeysScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }

    fn can_absorb_select(&self) -> bool {
        false
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "BlobStoreCheckpointedKeysScanOperator".to_string(),
            format!("Store = {:?}", self.config),
            format!("Schema = {}", self.schema.short_string()),
        ]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        let CheckpointStoreConfig::ObjectStore { prefix, io_config } = &self.config else {
            return Err(DaftError::External(
                format!("unsupported checkpoint config variant: {:?}", self.config).into(),
            ));
        };

        let rt = get_io_runtime(true);
        let prefix = prefix.clone();
        let io_config_arc = std::sync::Arc::new(io_config.as_ref().clone());
        let paths = rt
            .block_on_current_thread(async move {
                let store =
                    crate::impls::S3CheckpointStore::new(prefix, io_config_arc).map_err(|e| {
                        crate::error::CheckpointError::Internal {
                            message: format!("failed to build S3CheckpointStore: {e}"),
                        }
                    })?;
                store.sealed_file_paths().await
            })
            .map_err(|e| {
                DaftError::External(
                    format!("failed to enumerate sealed checkpoint key files: {e}").into(),
                )
            })?;

        let source_config = Arc::new(SourceConfig::File(FileFormatConfig::Parquet(
            ParquetSourceConfig::default(),
        )));
        let storage_config = Arc::new(StorageConfig::new_internal(
            true,
            Some(io_config.as_ref().clone()),
        ));
        let schema = self.schema.clone();

        Ok(paths
            .into_iter()
            .map(|path| {
                Arc::new(ScanTask::new(
                    vec![ScanSource {
                        size_bytes: None,
                        metadata: None,
                        statistics: None,
                        partition_spec: None,
                        kind: ScanSourceKind::File {
                            path,
                            chunk_spec: None::<ChunkSpec>,
                            iceberg_delete_files: None,
                            parquet_metadata: None,
                        },
                    }],
                    source_config.clone(),
                    schema.clone(),
                    storage_config.clone(),
                    pushdowns.clone(),
                    None,
                ))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use common_io_config::IOConfig;
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;
    use crate::{CheckpointId, CheckpointStore, impls::S3CheckpointStore, test_utils::keys};

    fn key_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8)]))
    }

    fn make_store(dir: &std::path::Path) -> (CheckpointStoreConfig, S3CheckpointStore) {
        let prefix = format!("file://{}", dir.display());
        let config = CheckpointStoreConfig::ObjectStore {
            prefix: prefix.clone(),
            io_config: Box::new(IOConfig::default()),
        };
        let store = S3CheckpointStore::new(prefix, StdArc::new(IOConfig::default())).unwrap();
        (config, store)
    }

    /// `to_scan_tasks` blocks internally; run it off the tokio test runtime.
    async fn to_scan_tasks_off_runtime(
        op: BlobStoreCheckpointedKeysScanOperator,
    ) -> DaftResult<Vec<ScanTaskRef>> {
        tokio::task::spawn_blocking(move || op.to_scan_tasks(Pushdowns::default()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn empty_store_produces_no_scan_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let (config, _store) = make_store(dir.path());
        let op = BlobStoreCheckpointedKeysScanOperator::new(config, key_schema());
        let tasks = to_scan_tasks_off_runtime(op).await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn sealed_checkpoints_produce_one_task_per_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let (config, store) = make_store(dir.path());

        let id1 = CheckpointId::generate(0);
        store.stage_keys(&id1, keys(&["a", "b"])).await.unwrap();
        store.stage_keys(&id1, keys(&["c"])).await.unwrap();
        store.checkpoint(&id1).await.unwrap();

        let id2 = CheckpointId::generate(1);
        store.stage_keys(&id2, keys(&["d"])).await.unwrap();
        store.checkpoint(&id2).await.unwrap();

        // Staged-only — should be invisible.
        let id3 = CheckpointId::generate(2);
        store.stage_keys(&id3, keys(&["ignored"])).await.unwrap();

        let mut expected_paths = store.sealed_file_paths().await.unwrap();
        expected_paths.sort();

        let op = BlobStoreCheckpointedKeysScanOperator::new(config, key_schema());
        let tasks = to_scan_tasks_off_runtime(op).await.unwrap();

        let mut task_paths: Vec<String> = tasks
            .iter()
            .map(|t| match &t.sources[0].kind {
                ScanSourceKind::File { path, .. } => path.clone(),
                other => panic!("expected File scan source, got: {other:?}"),
            })
            .collect();
        task_paths.sort();

        assert_eq!(task_paths, expected_paths);
    }

    #[tokio::test]
    async fn advertised_schema_is_passed_through_verbatim() {
        // The scan operator advertises whatever schema is handed to it, so
        // callers (the optimizer rule) are responsible for passing the
        // canonical sealed-keys column name. Pin that the operator does
        // expose the schema given at construction — a regression that
        // rewrote the schema internally would silently break the anti-join's
        // right-side resolution.
        use common_checkpoint_config::SEALED_KEYS_COLUMN;

        let dir = tempfile::tempdir().unwrap();
        let (config, _store) = make_store(dir.path());

        let canonical_schema = Arc::new(Schema::new(vec![Field::new(
            SEALED_KEYS_COLUMN,
            DataType::Utf8,
        )]));
        let op = BlobStoreCheckpointedKeysScanOperator::new(config, canonical_schema);

        let advertised = op.schema();
        assert_eq!(
            advertised.len(),
            1,
            "expected single-column schema, got: {advertised:?}"
        );
        assert!(
            advertised.get_field(SEALED_KEYS_COLUMN).is_ok(),
            "advertised schema must carry the canonical column name; got: {advertised:?}"
        );
    }
}
