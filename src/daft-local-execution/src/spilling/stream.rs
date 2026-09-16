use std::collections::VecDeque;

use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;

use super::{SpillError, SpillFile, SpillManager, SpillPartitionWriter, SpillScopeId};
use crate::memory_size::array_bytes;

pub(super) const TARGET_BLOCK_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const TARGET_FILE_BYTES: u64 = 128 * 1024 * 1024;

/// Appends one logical stream across input partitions. Blocks control the size of
/// individual IPC operations; files roll independently and can contain many blocks.
/// These are byte targets, not allocation permits or a bound on pickle execution.
pub(crate) struct SpillStreamWriter {
    manager: SpillManager,
    scope: SpillScopeId,
    schema: SchemaRef,
    writer: Option<SpillPartitionWriter>,
    files: VecDeque<SpillFile>,
    read_bytes: u64,
    block_bytes: u64,
    file_bytes: u64,
}

impl SpillStreamWriter {
    pub(crate) fn new(manager: SpillManager, scope: SpillScopeId, schema: SchemaRef) -> Self {
        Self {
            manager,
            scope,
            schema,
            writer: None,
            files: VecDeque::new(),
            read_bytes: 0,
            block_bytes: TARGET_BLOCK_BYTES,
            file_bytes: TARGET_FILE_BYTES,
        }
    }

    pub(crate) async fn append(mut self, partition: MicroPartition) -> Result<Self, SpillError> {
        if partition.schema() != self.schema {
            return Err(SpillError::Data("spill batch schema mismatch".to_string()));
        }
        for batch in partition.record_batches() {
            if batch.is_empty() {
                continue;
            }
            let batch = super::convert_batch(
                batch.clone(),
                self.manager.inner.io_runtime.io_slots.clone(),
            )
            .await?;
            let mut start = 0;
            while start < batch.num_rows() {
                let rows = block_rows(
                    &batch,
                    start,
                    self.block_bytes,
                    self.manager.inner.io_runtime.memory_pool.limit_bytes(),
                )?;
                let writer = match self.writer.take() {
                    Some(writer) => writer,
                    None => {
                        self.manager
                            .partition_writer(self.scope.clone(), self.schema.clone())
                            .await?
                    }
                };
                let writer = writer.append_arrow(batch.slice(start, rows)).await?;
                self.read_bytes = self.read_bytes.max(writer.read_bytes());
                if writer.bytes_written() >= self.file_bytes {
                    self.files.push_back(writer.finish().await?);
                } else {
                    self.writer = Some(writer);
                }
                start += rows;
            }
        }
        Ok(self)
    }

    pub(crate) async fn finish(mut self) -> Result<(VecDeque<SpillFile>, u64), SpillError> {
        if let Some(writer) = self.writer.take() {
            self.files.push_back(writer.finish().await?);
        }
        Ok((self.files, self.read_bytes))
    }
}

pub(super) fn block_rows(
    batch: &arrow_array::RecordBatch,
    start: usize,
    target: u64,
    workspace_limit: u64,
) -> Result<usize, SpillError> {
    // Match append_arrow's workspace estimate. A smaller workspace reduces block
    // size, not file size. Oversized single rows fail admission before IPC encoding.
    let available =
        workspace_limit.saturating_sub(super::schema_working_bytes(batch.schema().as_ref()));
    let target = target.min(available / super::ENCODING_BUFFER_FACTOR);
    let mut low = 1;
    let mut high = batch.num_rows() - start;
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let bytes = batch
            .columns()
            .iter()
            .try_fold(0_u64, |sum, array| {
                array_bytes(array.slice(start, mid).as_ref()).map(|bytes| sum.saturating_add(bytes))
            })
            .map_err(|error| SpillError::Data(error.to_string()))?;
        if bytes <= target {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    // A row is indivisible. Its actual frame size is included in read_bytes, so
    // admission rejects an impossible restore instead of under-reserving it.
    Ok(low)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field, Schema};
    use daft_memory::MemoryManager;
    use daft_recordbatch::RecordBatch;

    use super::*;

    #[tokio::test]
    async fn shared_workspace_progresses_with_execution_memory_full() {
        let root =
            std::env::temp_dir().join(format!("daft-spill-workspace-{}", uuid::Uuid::new_v4()));
        let memory = MemoryManager::with_spill_reserve(8 * 1024 * 1024, 1024 * 1024).unwrap();
        let runtime =
            crate::spilling::SpillIoRuntime::new([root.clone()], 1, memory.spill_pool()).unwrap();
        let first = SpillManager::for_execution(runtime.clone(), "first");
        let second = SpillManager::for_execution(runtime.clone(), "second");
        let execution = memory.reserve(7 * 1024 * 1024).await.unwrap();
        let held_workspace = memory.spill_pool().reserve(1024 * 1024).await.unwrap();
        let input = partition(0, 4096, 256);
        let mut pending = Box::pin(first.partition_writer(first.scope(0, 0), input.schema()));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(
            runtime.io_slots.available_permits(),
            1,
            "workspace wait must not hold I/O slots"
        );
        drop(held_workspace);
        drop(pending.await.unwrap());
        // Both executions draw from the same workspace while ordinary capacity is full.
        for manager in [&first, &second] {
            let writer =
                SpillStreamWriter::new(manager.clone(), manager.scope(0, 0), input.schema());
            let (files, _) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                writer
                    .append(input.clone())
                    .await
                    .unwrap()
                    .finish()
                    .await
                    .unwrap()
            })
            .await
            .expect("spill must progress without ordinary memory");
            assert_eq!(
                files.len(),
                1,
                "a smaller workspace must not force small files"
            );
            assert_eq!(memory.spill_pool().used_bytes(), 0);
            assert_eq!(memory.used_bytes(), execution.bytes());
            drop(files);
        }
        assert!(memory.spill_pool().peak_bytes() > 0);
        assert!(memory.spill_pool().peak_bytes() <= 1024 * 1024);
        drop(execution);
        assert_eq!(memory.used_bytes(), 0);
        drop(first);
        drop(second);
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn partition(start: i64, rows: usize, payload_bytes: usize) -> MicroPartition {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new("payload", DataType::Utf8),
        ]));
        let batch = RecordBatch::from_arrow(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from_iter_values(
                    start..start + rows as i64,
                )),
                Arc::new(arrow_array::LargeStringArray::from_iter_values(
                    (0..rows).map(|_| "x".repeat(payload_bytes)),
                )),
            ],
        )
        .unwrap();
        MicroPartition::new_loaded(schema, Arc::new(vec![batch]), None)
    }

    #[tokio::test]
    async fn blocks_and_input_partitions_do_not_each_create_a_file() {
        for block_bytes in [4 * 1024, 32 * 1024] {
            let root =
                std::env::temp_dir().join(format!("daft-spill-stream-{}", uuid::Uuid::new_v4()));
            let manager = SpillManager::with_io_concurrency([root.clone()], 1, "test").unwrap();
            let mut writer = SpillStreamWriter::new(
                manager.clone(),
                manager.scope(0, 0),
                partition(0, 0, 0).schema(),
            );
            writer.block_bytes = block_bytes;
            writer.file_bytes = 256 * 1024;
            for batch in 0..100 {
                writer = writer.append(partition(batch * 32, 32, 256)).await.unwrap();
            }
            let (files, read_bytes) = writer.finish().await.unwrap();
            assert!(files.len() < 10, "small blocks must share files");
            assert!(files.len() > 1, "files must still roll at the byte target");
            for file in files.iter().take(files.len() - 1) {
                assert!(file.len() >= 256 * 1024);
            }
            assert!(read_bytes < 64 * 1024);
            let memory = MemoryManager::new(read_bytes);
            let mut expected = 0;
            for file in files {
                let mut reader = manager
                    .open_micropartitions(&file, |bytes| memory.reserve(bytes))
                    .await
                    .unwrap();
                drop(file);
                while let Some((batch, permit, next)) = reader
                    .next_batch(|bytes| memory.reserve(bytes))
                    .await
                    .unwrap()
                {
                    for row in 0..batch.len() {
                        assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(expected));
                        expected += 1;
                    }
                    drop(batch);
                    drop(permit);
                    reader = next;
                }
                assert_eq!(memory.used_bytes(), 0);
            }
            assert_eq!(expected, 3200);
            drop(manager);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn large_append_records_largest_frame_not_total_written_bytes() {
        let root = std::env::temp_dir().join(format!("daft-spill-stream-{}", uuid::Uuid::new_v4()));
        let manager = SpillManager::new([root.clone()], "test").unwrap();
        let input = partition(0, 4096, 256);
        let mut writer =
            SpillStreamWriter::new(manager.clone(), manager.scope(0, 0), input.schema());
        writer.block_bytes = 32 * 1024;
        let (files, read_bytes) = writer.append(input).await.unwrap().finish().await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].len() > 1024 * 1024);
        assert!(read_bytes < 80 * 1024);
        let memory = MemoryManager::new(read_bytes);
        let mut reader = manager
            .open_micropartitions(&files[0], |bytes| memory.reserve(bytes))
            .await
            .unwrap();
        let mut rows = 0;
        while let Some((batch, permit, next)) = reader
            .next_batch(|bytes| memory.reserve(bytes))
            .await
            .unwrap()
        {
            rows += batch.len();
            drop(batch);
            drop(permit);
            reader = next;
        }
        assert_eq!(rows, 4096);
        assert_eq!(memory.used_bytes(), 0);
        drop(files);
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn schema_error_cleans_up_rolled_files_and_pending_tail() {
        let root = std::env::temp_dir().join(format!("daft-spill-stream-{}", uuid::Uuid::new_v4()));
        let manager = SpillManager::new([root.clone()], "test").unwrap();
        let input = partition(0, 101, 256);
        let mut writer =
            SpillStreamWriter::new(manager.clone(), manager.scope(0, 0), input.schema());
        writer.block_bytes = 4096;
        writer.file_bytes = 16 * 1024;
        let writer = writer.append(input).await.unwrap();
        let mut paths: Vec<_> = writer
            .files
            .iter()
            .map(|file| file.path().to_owned())
            .collect();
        assert!(!paths.is_empty());
        paths.push(
            writer
                .writer
                .as_ref()
                .unwrap()
                .writer
                .temporary_path
                .clone(),
        );
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("other", DataType::Int64)]));
        assert!(
            writer
                .append(MicroPartition::empty(Some(wrong_schema)))
                .await
                .is_err()
        );
        assert!(paths.iter().all(|path| !path.exists()));
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}
