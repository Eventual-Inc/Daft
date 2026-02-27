use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_io::{SourceType, parse_url};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_writers::{AsyncFileWriter, make_ipc_writer};
use itertools::Itertools;
use tokio::sync::Mutex;

fn get_shuffle_dirs(shuffle_dirs: &[String], cache_id: String, shuffle_id: u64) -> Vec<String> {
    shuffle_dirs
        .iter()
        .map(|dir| format!("{}/daft_shuffle/{}/{}", dir, shuffle_id, cache_id))
        .collect()
}

fn get_partition_dir(shuffle_dirs: &[String], partition_idx: usize) -> String {
    let dir = &shuffle_dirs[partition_idx % shuffle_dirs.len()];
    format!("{}/partition_{}", dir, partition_idx)
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
    writer_senders: Option<Vec<async_channel::Sender<Arc<MicroPartition>>>>,
    writer_tasks: Vec<WriterTask>,
    error: Option<String>,
}

pub struct InProgressShuffleCache {
    state: Mutex<InProgressShuffleCacheState>,
    writer_senders_weak: Vec<async_channel::WeakSender<Arc<MicroPartition>>>,
    shuffle_dirs: Vec<String>,
    cache_id: String,
}

impl InProgressShuffleCache {
    pub fn try_new(
        num_partitions: usize,
        dirs: &[String],
        cache_id: String,
        shuffle_id: u64,
        target_filesize: usize,
        compression: Option<&str>,
    ) -> DaftResult<Self> {
        // Create the directories
        // TODO: Add checks here, as well as periodic checks to ensure that the dirs are not too full. If so, we switch to directories with more space.
        // And raise an error if we can't find any directories with space.
        let shuffle_dirs = get_shuffle_dirs(dirs, cache_id.clone(), shuffle_id);
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
            if std::path::Path::new(dir).exists() {
                std::fs::remove_dir_all(dir)?;
            }
            std::fs::create_dir_all(dir)?;
        }

        // Create the partition writers
        let mut writers = Vec::with_capacity(num_partitions);
        for partition_idx in 0..num_partitions {
            let partition_dir = get_partition_dir(&shuffle_dirs, partition_idx);
            std::fs::create_dir_all(&partition_dir)?;

            let writer = make_ipc_writer(&partition_dir, target_filesize, compression)?;
            writers.push(writer);
        }

        // Create the InProgressShuffleCache with the writers
        Self::try_new_with_writers(writers, shuffle_dirs, cache_id)
    }

    fn try_new_with_writers(
        writers: Vec<
            Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
        >,
        shuffle_dirs: Vec<String>,
        cache_id: String,
    ) -> DaftResult<Self> {
        let num_cpus = std::thread::available_parallelism().unwrap().get();

        // Spawn the writer tasks
        let (writer_tasks, writer_senders): (Vec<_>, Vec<_>) = writers
            .into_iter()
            .map(|writer| {
                // Use a bounded channel with capacity based on number of CPUs
                // This allows multiple concurrent partitioners to send data without blocking
                let (tx, rx) = async_channel::bounded(num_cpus * 2);
                let task = get_io_runtime(true).spawn(async move { writer_task(rx, writer).await });
                (task, tx)
            })
            .unzip();

        // Store weak senders so we can access them without locking
        let weak_senders: Vec<_> = writer_senders.iter().map(|s| s.downgrade()).collect();

        Ok(Self {
            state: Mutex::new(InProgressShuffleCacheState {
                writer_senders: Some(writer_senders),
                writer_tasks,
                error: None,
            }),
            writer_senders_weak: weak_senders,
            shuffle_dirs,
            cache_id,
        })
    }

    /// Push already-partitioned data to the writers.
    /// The input should be a Vec<Arc<MicroPartition>> where each element corresponds to a writer.
    pub async fn push_partitioned_data(
        &self,
        partitioned_data: Vec<Arc<MicroPartition>>,
    ) -> DaftResult<()> {
        // Verify we have the right number of partitions
        if partitioned_data.len() != self.writer_senders_weak.len() {
            return Err(DaftError::ValueError(format!(
                "Expected {} partitions in shuffle cache, got {}",
                self.writer_senders_weak.len(),
                partitioned_data.len()
            )));
        }

        // Upgrade weak senders and send data
        let send_futures = partitioned_data
            .into_iter()
            .zip(self.writer_senders_weak.iter())
            .map(|(partition, weak_sender)| async move {
                // Try to upgrade the weak sender
                match weak_sender.upgrade() {
                    Some(sender) => sender.send(partition).await.map_err(|e| e.to_string()),
                    None => {
                        // Sender has been dropped, meaning the cache is closed
                        Err("Shuffle cache has been closed".to_string())
                    }
                }
            });

        // If any send fails, close the cache to get the error
        if let Err(e) = futures::future::try_join_all(send_futures).await {
            self.close().await?;
            return Err(DaftError::InternalError(e));
        }

        Ok(())
    }

    pub async fn close(&self) -> DaftResult<ShuffleCache> {
        let mut state = self.state.lock().await;
        // If there was an error from a previous close, return it
        if let Some(error) = &state.error {
            return Err(DaftError::InternalError(error.clone()));
        }

        let writer_senders = state
            .writer_senders
            .take()
            .expect("writer_senders should be present");
        let writer_tasks = std::mem::take(&mut state.writer_tasks);

        // Close the writer tasks
        let close_result = Self::close_internal(writer_senders, writer_tasks).await;
        if let Err(err) = close_result {
            state.error = Some(err.to_string());
            return Err(err);
        }

        // All good, get the schema and results
        let writer_results = close_result.unwrap();

        let schema = writer_results
            .iter()
            .find_map(|result| result.schema.clone())
            .unwrap_or_else(|| {
                panic!("No schema found in shuffle cache, this should never happen")
            });
        let (
            bytes_per_file_per_partition,
            file_paths_per_partition,
            rows_per_partition,
            bytes_per_partition,
        ) = writer_results
            .into_iter()
            .map(|result| {
                (
                    result.bytes_per_file,
                    result.file_paths,
                    result.total_rows_written,
                    result.total_bytes_written,
                )
            })
            .multiunzip();

        Ok(ShuffleCache::new(
            schema,
            bytes_per_file_per_partition,
            file_paths_per_partition,
            rows_per_partition,
            bytes_per_partition,
            self.shuffle_dirs.clone(),
            self.cache_id.clone(),
        ))
    }

    async fn close_internal(
        writer_senders: Vec<async_channel::Sender<Arc<MicroPartition>>>,
        writer_tasks: Vec<WriterTask>,
    ) -> DaftResult<Vec<WriterTaskResult>> {
        // Drop the writer senders so that the writer tasks can exit
        drop(writer_senders);

        // Wait for the writer tasks to exit
        let results = futures::future::try_join_all(writer_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(results)
    }
}

// Writer task that takes partitions from a writer sender, writes them to a file, and returns the schema and file paths
async fn writer_task(
    rx: async_channel::Receiver<Arc<MicroPartition>>,
    mut writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
) -> DaftResult<WriterTaskResult> {
    let io_runtime = get_io_runtime(true);
    let mut schema = None;
    let mut total_rows_written = 0;
    let mut total_bytes_written = 0;
    while let Ok(partition) = rx.recv().await {
        if schema.is_none() {
            schema = Some(partition.schema().clone());
        }
        total_rows_written += partition.len();
        total_bytes_written += partition.size_bytes();
        writer = io_runtime
            .spawn(async move {
                writer.write(partition).await?;
                DaftResult::Ok(writer)
            })
            .await??;
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
    Ok(WriterTaskResult {
        schema,
        bytes_per_file,
        total_rows_written,
        total_bytes_written,
        file_paths,
    })
}

#[derive(Debug)]
pub struct ShuffleCache {
    schema: SchemaRef,
    bytes_per_file_per_partition: Vec<Vec<usize>>,
    file_paths_per_partition: Vec<Vec<String>>,
    rows_per_partition: Vec<usize>,
    bytes_per_partition: Vec<usize>,
    shuffle_dirs: Vec<String>,
    cache_id: String,
}

impl ShuffleCache {
    pub fn new(
        schema: SchemaRef,
        bytes_per_file_per_partition: Vec<Vec<usize>>,
        file_paths_per_partition: Vec<Vec<String>>,
        rows_per_partition: Vec<usize>,
        bytes_per_partition: Vec<usize>,
        shuffle_dirs: Vec<String>,
        cache_id: String,
    ) -> Self {
        Self {
            schema,
            bytes_per_file_per_partition,
            file_paths_per_partition,
            rows_per_partition,
            bytes_per_partition,
            shuffle_dirs,
            cache_id,
        }
    }

    pub fn cache_id(&self) -> &str {
        &self.cache_id
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn file_paths_for_partition(&self, partition_idx: usize) -> Vec<String> {
        self.file_paths_per_partition[partition_idx].clone()
    }

    pub fn bytes_per_file_for_partition(&self, partition_idx: usize) -> Vec<usize> {
        self.bytes_per_file_per_partition[partition_idx].clone()
    }

    pub fn rows_per_partition(&self) -> Vec<usize> {
        self.rows_per_partition.clone()
    }

    pub fn bytes_per_partition(&self) -> Vec<usize> {
        self.bytes_per_partition.clone()
    }

    pub fn clear_partition(&self, partition_idx: usize) -> DaftResult<()> {
        let partition_dir = get_partition_dir(&self.shuffle_dirs, partition_idx);
        std::fs::remove_dir_all(partition_dir)?;
        Ok(())
    }

    pub fn clear_directories(&self) -> DaftResult<()> {
        for dir in &self.shuffle_dirs {
            std::fs::remove_dir_all(dir)?;
        }
        Ok(())
    }
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
        // Create dummy writers for testing
        let num_partitions = 1;
        let mut writers = Vec::with_capacity(num_partitions);
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        for partition_idx in 0..num_partitions {
            writers.push(dummy_writer_factory.create_writer(partition_idx, None)?);
        }

        // Create the cache with dummy writers
        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            vec![],
            "test_cache".to_string(),
        )?;

        // Create and push some partitions
        // Since we have 1 partition, all data goes to partition 0
        let mp1 = make_dummy_mp(100);
        let mp2 = make_dummy_mp(200);

        cache.push_partitioned_data(vec![mp1]).await?;
        cache.push_partitioned_data(vec![mp2]).await?;

        // Close the cache and verify results
        let shuffle_cache = cache.close().await?;

        assert_eq!(shuffle_cache.file_paths_per_partition.len(), num_partitions);

        // We should have 3 file paths because we wrote 300 bytes and the target filesize is 100
        for paths in &shuffle_cache.file_paths_per_partition {
            assert_eq!(paths.len(), 3);
        }

        // Check that bytes were distributed
        let total_bytes: usize = shuffle_cache
            .bytes_per_file_per_partition
            .iter()
            .flat_map(|bytes| bytes.iter())
            .sum();

        // We should have recorded bytes for our two micropartitions
        assert!(total_bytes == 300);

        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_with_partition_by() -> DaftResult<()> {
        // Create dummy writers for testing
        let num_partitions = 2;
        let mut writers = Vec::with_capacity(num_partitions);
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        for partition_idx in 0..num_partitions {
            writers.push(dummy_writer_factory.create_writer(partition_idx, None)?);
        }

        // Create the cache with dummy writers
        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            vec![],
            "test_cache".to_string(),
        )?;

        // Create and push some partitions
        // For testing, we'll manually distribute data across partitions
        let mp = make_dummy_mp(150);
        let mp2 = make_dummy_mp(350);

        // Distribute data across 2 partitions (simplified for testing)
        cache.push_partitioned_data(vec![mp, mp2]).await?;

        // Close the cache and verify results
        let shuffle_cache = cache.close().await?;

        assert_eq!(shuffle_cache.file_paths_per_partition.len(), num_partitions);

        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_with_empty_partitions() -> DaftResult<()> {
        let num_partitions = 5;
        let mut writers = Vec::with_capacity(num_partitions);
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        for partition_idx in 0..num_partitions {
            writers.push(dummy_writer_factory.create_writer(partition_idx, None)?);
        }

        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            vec![],
            "test_cache".to_string(),
        )?;

        // 1000 empty partitions, distributed across 5 writers
        for _ in 0..1000 {
            let empty_partitions: Vec<_> = (0..num_partitions).map(|_| make_dummy_mp(0)).collect();
            cache.push_partitioned_data(empty_partitions).await?;
        }

        let shuffle_cache = cache.close().await?;

        // Even though we pushed empty partitions, we should still have the schema
        assert!(shuffle_cache.schema().names() == vec!["ints"]);
        assert_eq!(shuffle_cache.file_paths_per_partition.len(), num_partitions);
        assert!(
            shuffle_cache
                .file_paths_per_partition
                .iter()
                .all(|paths| paths.len() == 0),
            "All partitions should have no file paths: {:?}",
            shuffle_cache.file_paths_per_partition
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_shuffle_cache_with_failing_writer() -> DaftResult<()> {
        // Create failing writers for testing
        let num_partitions = 2;
        let mut writers = Vec::with_capacity(num_partitions);

        // First writer fails on write
        let failing_writer_factory = FailingWriterFactory::new_fail_on_write();
        let failing_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(failing_writer_factory));
        writers.push(failing_writer_factory.create_writer(0, None)?);
        writers.push(failing_writer_factory.create_writer(1, None)?);

        // Create the cache with writers
        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            vec![],
            "test_cache".to_string(),
        )?;

        let mut found_failure = false;
        // Technically, we can calculate the max number of iterations before failure, based on number of tasks and channel sizes,
        // but 100 should be good enough based on our testing environment.
        let num_iterations = 100;
        for _ in 0..num_iterations {
            let partitions = vec![make_dummy_mp(100), make_dummy_mp(100)];
            if let Err(err) = cache.push_partitioned_data(partitions).await {
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
        let partitions = vec![make_dummy_mp(100), make_dummy_mp(100)];
        let result = cache.push_partitioned_data(partitions).await;
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
        let num_partitions = 2;
        let mut writers = Vec::with_capacity(num_partitions);

        // First writer fails on close
        let failing_writer_factory = FailingWriterFactory::new_fail_on_close();
        let failing_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(failing_writer_factory));
        writers.push(failing_writer_factory.create_writer(0, None)?);
        writers.push(failing_writer_factory.create_writer(1, None)?);

        // Create the cache with writers
        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            vec![],
            "test_cache".to_string(),
        )?;

        // Create and push a partition
        let partitions = vec![make_dummy_mp(100), make_dummy_mp(100)];

        // This should succeed since the failure happens on close
        cache.push_partitioned_data(partitions).await?;

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
        let partitions = vec![make_dummy_mp(100), make_dummy_mp(100)];
        let result = cache.push_partitioned_data(partitions).await;
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
