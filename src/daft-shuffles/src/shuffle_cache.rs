use std::sync::{Arc, OnceLock};

use common_error::{DaftError, DaftResult};
use common_metrics::SpillReporter;
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_io::{SourceType, parse_url};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_writers::{AsyncFileWriter, make_ipc_writer};
use tokio::sync::Mutex;

fn get_shuffle_dirs(shuffle_dirs: &[String], shuffle_id: u64) -> Vec<String> {
    shuffle_dirs
        .iter()
        .map(|dir| format!("{}/daft_shuffle/{}", dir, shuffle_id))
        .collect()
}

fn get_partition_dir(shuffle_dirs: &[String], partition_ref_id: u64) -> String {
    let dir = &shuffle_dirs[(partition_ref_id as usize) % shuffle_dirs.len()];
    format!("{}/partition_ref_{}", dir, partition_ref_id)
}

pub fn partition_ref_id(input_id: u32, partition_idx: usize) -> u64 {
    ((input_id as u64) << 32) | partition_idx as u64
}

// Result of a writer task
struct WriterTaskResult {
    schema: Option<SchemaRef>,
    bytes_per_file: Vec<usize>,
    total_rows_written: usize,
    total_bytes_written: usize,
    file_paths: Vec<String>,
}
type WriterTask = RuntimeTask<DaftResult<WriterTaskResult>>;

struct InProgressShuffleCacheState {
    writer_sender: Option<async_channel::Sender<MicroPartition>>,
    writer_task: Option<WriterTask>,
    error: Option<String>,
}

pub struct InProgressShuffleCache {
    state: Mutex<InProgressShuffleCacheState>,
    writer_sender_weak: async_channel::WeakSender<MicroPartition>,
    partition_ref_id: u64,
    /// Set at most once by the owning sink after the cache is constructed.
    /// The writer task holds an `Arc` clone and reads it on each write —
    /// when unset, spill I/O isn't recorded. Allows eager cache construction
    /// to stay cheap while `SpillReporter` attachment happens later in
    /// `sink()` where `runtime_stats` is available.
    spill: Arc<OnceLock<SpillReporter>>,
}

impl InProgressShuffleCache {
    pub fn try_new(
        partition_ref_id: u64,
        dirs: &[String],
        shuffle_id: u64,
        target_filesize: usize,
        compression: Option<&str>,
    ) -> DaftResult<Self> {
        // Create the directories
        // TODO: Add checks here, as well as periodic checks to ensure that the dirs are not too full. If so, we switch to directories with more space.
        // And raise an error if we can't find any directories with space.
        let shuffle_dirs = get_shuffle_dirs(dirs, shuffle_id);
        for dir in &shuffle_dirs {
            // Check that the dir is a file
            let (source_type, _) = parse_url(dir)?;
            if source_type != SourceType::File {
                return Err(DaftError::ValueError(format!(
                    "ShuffleCache only supports file paths, got: {}",
                    dir
                )));
            }

            // If the directory doesn't exist, create it
            if !std::path::Path::new(dir).exists() {
                std::fs::create_dir_all(dir)?;
            }
        }

        // Create the partition writer
        let partition_dir = get_partition_dir(&shuffle_dirs, partition_ref_id);
        std::fs::create_dir_all(&partition_dir)?;

        let writer = make_ipc_writer(&partition_dir, target_filesize, compression)?;

        Self::try_new_with_writer(writer, partition_ref_id)
    }

    fn try_new_with_writer(
        writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
        partition_ref_id: u64,
    ) -> DaftResult<Self> {
        let num_cpus = std::thread::available_parallelism().unwrap().get();
        let (tx, rx) = async_channel::bounded(num_cpus * 2);
        let spill: Arc<OnceLock<SpillReporter>> = Arc::new(OnceLock::new());
        let spill_for_task = spill.clone();
        let task = get_io_runtime(true)
            .spawn(async move { writer_task(rx, writer, spill_for_task).await });

        let writer_sender_weak = tx.downgrade();

        Ok(Self {
            state: Mutex::new(InProgressShuffleCacheState {
                writer_sender: Some(tx),
                writer_task: Some(task),
                error: None,
            }),
            writer_sender_weak,
            partition_ref_id,
            spill,
        })
    }

    /// Attach a `SpillReporter` so subsequent writes are accounted for against
    /// the owning operator's runtime stats. No-op if already attached (only
    /// the first caller wins — later calls are silently ignored).
    pub fn set_spill_reporter(&self, reporter: SpillReporter) {
        let _ = self.spill.set(reporter);
    }

    /// Push single partition data to the writer.
    pub async fn push_partition_data(&self, partition: MicroPartition) -> DaftResult<()> {
        let send_future = async move {
            match self.writer_sender_weak.upgrade() {
                Some(sender) => sender.send(partition).await.map_err(|e| e.to_string()),
                None => Err("Shuffle cache has been closed".to_string()),
            }
        };

        if let Err(e) = send_future.await {
            self.close().await?;
            return Err(DaftError::InternalError(e));
        }

        Ok(())
    }

    pub async fn close(&self) -> DaftResult<PartitionCache> {
        let mut state = self.state.lock().await;
        // If there was an error from a previous close, return it
        if let Some(error) = &state.error {
            return Err(DaftError::InternalError(error.clone()));
        }

        let writer_sender = state.writer_sender.take();
        let writer_task = std::mem::take(&mut state.writer_task);

        // Close the writer tasks
        let close_result = Self::close_internal(writer_sender, writer_task).await;
        if let Err(err) = close_result {
            state.error = Some(err.to_string());
            return Err(err);
        }

        // All good, get the schema and results
        let writer_result = close_result.unwrap();

        match writer_result {
            Some(result) => Ok(PartitionCache {
                partition_ref_id: self.partition_ref_id,
                schema: result.schema.unwrap_or_else(|| {
                    panic!("No schema found in shuffle cache, this should never happen");
                }),
                bytes_per_file: result.bytes_per_file,
                file_paths: result.file_paths,
                num_rows: result.total_rows_written,
                size_bytes: result.total_bytes_written,
            }),
            None => Err(DaftError::InternalError(
                "No writer result found".to_string(),
            )),
        }
    }

    async fn close_internal(
        writer_sender: Option<async_channel::Sender<MicroPartition>>,
        writer_task: Option<WriterTask>,
    ) -> DaftResult<Option<WriterTaskResult>> {
        // Drop the writer senders so that the writer tasks can exit
        drop(writer_sender);

        // Wait for the writer tasks to exit
        if let Some(writer_task) = writer_task {
            let result = writer_task.await??;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

// Writer task that takes a partition from a writer sender, writes them to a file, and returns the schema and file path
async fn writer_task(
    rx: async_channel::Receiver<MicroPartition>,
    mut writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
    spill: Arc<OnceLock<SpillReporter>>,
) -> DaftResult<WriterTaskResult> {
    let io_runtime = get_io_runtime(true);
    let mut schema = None;
    let mut total_rows_written = 0;
    let mut total_bytes_written = 0;
    while let Ok(partition) = rx.recv().await {
        if schema.is_none() {
            schema = Some(partition.schema().clone());
        }
        let bytes = partition.size_bytes();
        total_rows_written += partition.len();
        total_bytes_written += bytes;
        writer = io_runtime
            .spawn(async move {
                writer.write(partition).await?;
                DaftResult::Ok(writer)
            })
            .await??;
        if let Some(reporter) = spill.get() {
            reporter.record_bytes_written(bytes as u64);
        }
    }
    let file_path_tables = writer.close().await?;

    let file_paths = file_path_tables
        .into_iter()
        .map(|file_path_table| {
            assert!(file_path_table.num_columns() > 0);
            assert!(file_path_table.num_rows() == 1);
            // IPC writer should always return a RecordBatch of one path column
            let path = file_path_table
                .get_column(0)
                .utf8()?
                .get(0)
                .expect("path column should have one path");
            Ok(path.to_string())
        })
        .collect::<DaftResult<Vec<String>>>()?;

    let bytes_per_file = writer.bytes_per_file();
    assert!(bytes_per_file.len() == file_paths.len());
    if let Some(reporter) = spill.get() {
        for _ in 0..file_paths.len() {
            reporter.record_file_created();
        }
    }
    Ok(WriterTaskResult {
        schema,
        bytes_per_file,
        total_rows_written,
        total_bytes_written,
        file_paths,
    })
}

#[derive(Debug, Clone)]
pub struct PartitionCache {
    pub partition_ref_id: u64,
    pub schema: SchemaRef,
    pub bytes_per_file: Vec<usize>,
    pub file_paths: Vec<String>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_writers::test::{
        DummyWriterFactory, FailingWriterFactory, make_dummy_mp,
        make_dummy_target_file_size_writer_factory,
    };

    use super::*;

    #[tokio::test]
    async fn test_shuffle_cache_basic() -> DaftResult<()> {
        // Create dummy writer for testing
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        let writer = dummy_writer_factory.create_writer(0, None)?;

        // Create the cache with dummy writers
        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0)?;

        // Create and push some partitions
        // Since we have 1 partition, all data goes to partition 0
        let mp1 = make_dummy_mp(100);
        let mp2 = make_dummy_mp(200);

        cache.push_partition_data(mp1).await?;
        cache.push_partition_data(mp2).await?;

        // Close the cache and verify results
        let partition_cache = cache.close().await?;

        // We should have 3 file paths because we wrote 300 bytes and the target filesize is 100
        assert_eq!(partition_cache.file_paths.len(), 3);

        // Check that bytes were distributed
        let total_bytes: usize = partition_cache.bytes_per_file.iter().sum();

        // We should have recorded bytes for our two micropartitions
        assert!(total_bytes == 300);

        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_with_empty_partitions() -> DaftResult<()> {
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        let writer = dummy_writer_factory.create_writer(0, None)?;

        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0)?;

        // Push 1000 empty partitions
        for _ in 0..1000 {
            let empty_partition = make_dummy_mp(0);
            cache.push_partition_data(empty_partition).await?;
        }

        let partition_cache = cache.close().await?;

        // Even though we pushed empty partitions, we should still have the schema
        assert!(partition_cache.schema.names() == vec!["ints"]);
        assert_eq!(partition_cache.file_paths.len(), 0);
        assert!(
            partition_cache
                .file_paths
                .iter()
                .all(|paths| paths.is_empty()),
            "All partitions should have no file paths: {:?}",
            partition_cache.file_paths
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_shuffle_cache_with_failing_writer() -> DaftResult<()> {
        // Create failing writers for testing
        // First writer fails on write
        let failing_writer_factory = FailingWriterFactory::new_fail_on_write();
        let failing_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(failing_writer_factory));
        let writer = failing_writer_factory.create_writer(0, None)?;

        // Create the cache with writers
        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0)?;

        let mut found_failure = false;
        // Technically, we can calculate the max number of iterations before failure, based on number of tasks and channel sizes,
        // but 100 should be good enough based on our testing environment.
        let num_iterations = 100;
        for _ in 0..num_iterations {
            let partition = make_dummy_mp(100);
            if let Err(err) = cache.push_partition_data(partition).await {
                // Verify the error message
                let error_message = err.to_string();
                assert!(
                    error_message.contains("Intentional failure in FailingWriter::write"),
                    "Error message should mention write failure: {}",
                    error_message
                );
                found_failure = true;
                break;
            }
        }

        // Assert that the loop did not complete
        assert!(
            found_failure,
            "Expected failure before completing all pushes, num_iterations: {}",
            num_iterations
        );

        // Assert that another push will fail
        let partition = make_dummy_mp(100);
        let result = cache.push_partition_data(partition).await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::write"),
            "Error message should mention write failure: {}",
            error_message
        );

        // Try that closing the cache will fail
        let result = cache.close().await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::write"),
            "Error message should mention write failure: {}",
            error_message
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_with_failing_writer_on_close() -> DaftResult<()> {
        // Create failing writers for testing

        // First writer fails on close
        let failing_writer_factory = FailingWriterFactory::new_fail_on_close();
        let failing_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(failing_writer_factory));
        let writer = failing_writer_factory.create_writer(0, None)?;

        // Create the cache with writers
        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0)?;

        // Create and push a partition
        let partitions = vec![make_dummy_mp(100), make_dummy_mp(100)];

        for partition in partitions {
            // This should succeed since the failure happens on close
            cache.push_partition_data(partition).await?;
        }

        // When we close, we should get an error
        let result = cache.close().await;
        assert!(result.is_err());

        // Verify the error message
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::close"),
            "Error message should mention close failure: {}",
            error_message
        );

        // Try that another push will fail
        let partition = make_dummy_mp(100);
        let result = cache.push_partition_data(partition).await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::close"),
            "Error message should mention close failure: {}",
            error_message
        );

        // Try that closing the cache will fail
        let result = cache.close().await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::close"),
            "Error message should mention close failure: {}",
            error_message
        );

        Ok(())
    }
}
