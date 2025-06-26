use std::sync::{Arc, OnceLock};

use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_runtime, RuntimeRef, RuntimeTask};
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef};
use daft_io::{parse_url, SourceType};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_writers::{make_ipc_writer, AsyncFileWriter};
use itertools::Itertools;
use tokio::sync::Mutex;

// Single threaded runtime used for shuffle cache tasks, e.g. partitioner and writer tasks
static SHUFFLE_CACHE_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();

pub fn get_or_init_shuffle_cache_runtime() -> &'static RuntimeRef {
    SHUFFLE_CACHE_RUNTIME.get_or_init(|| {
        common_runtime::Runtime::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
            common_runtime::PoolType::Custom("shuffle_cache".to_string()),
        )
    })
}

fn get_shuffle_dirs(
    shuffle_dirs: &[String],
    node_id: &str,
    shuffle_stage_id: usize,
) -> Vec<String> {
    shuffle_dirs
        .iter()
        .map(|dir| {
            format!(
                "{}/daft_shuffle/node_{}/shuffle_stage_{}",
                dir, node_id, shuffle_stage_id
            )
        })
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
    partitioner_sender: Option<async_channel::Sender<Arc<MicroPartition>>>,
    partitioner_tasks: Vec<RuntimeTask<DaftResult<()>>>,
    writer_tasks: Vec<WriterTask>,
    error: Option<String>,
}

pub struct InProgressShuffleCache {
    state: Mutex<InProgressShuffleCacheState>,
    shuffle_dirs: Vec<String>,
    partitioner_sender_weak: async_channel::WeakSender<Arc<MicroPartition>>,
}

impl InProgressShuffleCache {
    pub fn try_new(
        num_partitions: usize,
        dirs: &[String],
        node_id: String,
        shuffle_stage_id: usize,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<ExprRef>>,
    ) -> DaftResult<Self> {
        // Create the directories
        // TODO: Add checks here, as well as periodic checks to ensure that the dirs are not too full. If so, we switch to directories with more space.
        // And raise an error if we can't find any directories with space.
        let shuffle_dirs = get_shuffle_dirs(dirs, &node_id, shuffle_stage_id);
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
        Self::try_new_with_writers(writers, num_partitions, partition_by, shuffle_dirs)
    }

    fn try_new_with_writers(
        writers: Vec<
            Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
        >,
        num_partitions: usize,
        partition_by: Option<Vec<ExprRef>>,
        shuffle_dirs: Vec<String>,
    ) -> DaftResult<Self> {
        let num_cpus = std::thread::available_parallelism().unwrap().get();

        // Spawn the writer tasks
        let (writer_tasks, writer_senders): (Vec<_>, Vec<_>) = writers
            .into_iter()
            .map(|writer| {
                let (tx, rx) = async_channel::bounded(num_cpus * 2);
                let task = get_or_init_shuffle_cache_runtime()
                    .spawn(async move { writer_task(rx, writer).await });
                (task, tx)
            })
            .unzip();

        // Spawn the partitioner tasks
        let (partitioner_sender, partitioner_receiver) = async_channel::bounded(1);
        let partitioner_tasks = (0..num_cpus)
            .map(|_| {
                let partitioner_receiver = partitioner_receiver.clone();
                let writer_senders = writer_senders.clone();
                let partition_by = partition_by.clone();
                get_or_init_shuffle_cache_runtime().spawn(async move {
                    partitioner_task(
                        partitioner_receiver,
                        writer_senders,
                        partition_by,
                        num_partitions,
                    )
                    .await
                })
            })
            .collect::<Vec<_>>();

        // Use a weak reference to the partitioner sender to avoid holding a strong reference to it.
        // We store the strong reference in locked state so that we can drop it on close
        let weak_partitioner_sender = partitioner_sender.downgrade();
        Ok(Self {
            state: Mutex::new(InProgressShuffleCacheState {
                partitioner_sender: Some(partitioner_sender),
                partitioner_tasks,
                writer_tasks,
                error: None,
            }),
            partitioner_sender_weak: weak_partitioner_sender,
            shuffle_dirs,
        })
    }

    pub async fn push_partitions(
        &self,
        input_partitions: Vec<Arc<MicroPartition>>,
    ) -> DaftResult<()> {
        // Try to get the partitioner sender from the weak reference
        match self.partitioner_sender_weak.upgrade() {
            Some(partitioner_sender) => {
                // Create futures for sending each partition
                let send_futures = input_partitions
                    .into_iter()
                    .map(|partition| partitioner_sender.send(partition));

                // If any send fails, the receiver is dropped, so we need to close the shuffle cache to get the error
                if let Err(e) = futures::future::try_join_all(send_futures).await {
                    self.close().await?;
                    return Err(DaftError::InternalError(e.to_string()));
                }
            }
            None => {
                // If the partitioner sender is dropped, that means the shuffle cache is closed
                // so we propagate the error from the close
                self.close().await?;
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> DaftResult<ShuffleCache> {
        let mut state = self.state.lock().await;
        // If there was an error from a previous close, return it
        if let Some(error) = &state.error {
            return Err(DaftError::InternalError(error.clone()));
        }
        let partitioner_sender = state.partitioner_sender.take().unwrap();
        let partitioner_tasks = std::mem::take(&mut state.partitioner_tasks);
        let writer_tasks = std::mem::take(&mut state.writer_tasks);

        // Close the partitioner tasks and writer tasks
        let close_result =
            Self::close_internal(partitioner_tasks, partitioner_sender, writer_tasks).await;
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
        ))
    }

    async fn close_internal(
        partitioner_tasks: Vec<RuntimeTask<DaftResult<()>>>,
        partitioner_sender: async_channel::Sender<Arc<MicroPartition>>,
        writer_tasks: Vec<WriterTask>,
    ) -> DaftResult<Vec<WriterTaskResult>> {
        // Drop the partitioner sender so that the partitioner tasks can exit
        drop(partitioner_sender);

        // Wait for the partitioner tasks to exit
        futures::future::try_join_all(partitioner_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<()>>()?;

        // Wait for the writer tasks to exit
        let results = futures::future::try_join_all(writer_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(results)
    }
}

// Partitioner task that takes partitions from the partitioner sender, partitions them, and sends them to the writer tasks
async fn partitioner_task(
    rx: async_channel::Receiver<Arc<MicroPartition>>,
    writer_senders: Vec<async_channel::Sender<Arc<MicroPartition>>>,
    partition_by: Option<Vec<ExprRef>>,
    num_partitions: usize,
) -> DaftResult<()> {
    let compute_runtime = get_compute_runtime();
    while let Ok(partition) = rx.recv().await {
        let partition_by = partition_by.clone();
        let partitions = compute_runtime
            .spawn(async move {
                let partitioned = match &partition_by {
                    Some(partition_by) => {
                        let partition_by = BoundExpr::bind_all(partition_by, &partition.schema())?;
                        partition.partition_by_hash(&partition_by, num_partitions)?
                    }
                    None => partition.partition_by_random(num_partitions, 0)?,
                };
                DaftResult::Ok(partitioned)
            })
            .await??;
        if futures::future::try_join_all(
            partitions
                .into_iter()
                .zip(writer_senders.iter())
                .map(|(partition, sender)| sender.send(partition.into())),
        )
        .await
        .is_err()
        {
            break;
        }
    }
    Ok(())
}

// Writer task that takes partitions from the partitioner sender, writes them to a file, and returns the schema and file paths
async fn writer_task(
    rx: async_channel::Receiver<Arc<MicroPartition>>,
    mut writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
) -> DaftResult<WriterTaskResult> {
    let compute_runtime = get_compute_runtime();
    let mut schema = None;
    let mut total_rows_written = 0;
    let mut total_bytes_written = 0;
    while let Ok(partition) = rx.recv().await {
        if schema.is_none() {
            schema = Some(partition.schema().clone());
        }
        total_rows_written += partition.len();
        total_bytes_written += partition.size_bytes()?.expect("size_bytes should be Some");
        writer = compute_runtime
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
}

impl ShuffleCache {
    pub fn new(
        schema: SchemaRef,
        bytes_per_file_per_partition: Vec<Vec<usize>>,
        file_paths_per_partition: Vec<Vec<String>>,
        rows_per_partition: Vec<usize>,
        bytes_per_partition: Vec<usize>,
        shuffle_dirs: Vec<String>,
    ) -> Self {
        Self {
            schema,
            bytes_per_file_per_partition,
            file_paths_per_partition,
            rows_per_partition,
            bytes_per_partition,
            shuffle_dirs,
        }
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

    use daft_dsl::{Column, Expr, ResolvedColumn};
    use daft_writers::test::{
        make_dummy_mp, make_dummy_target_file_size_writer_factory, DummyWriterFactory,
        FailingWriterFactory,
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
            num_partitions,
            None, // No partition by expressions
            vec![],
        )?;

        // Create and push some partitions
        let mp1 = make_dummy_mp(100);
        let mp2 = make_dummy_mp(200);

        cache.push_partitions(vec![mp1, mp2]).await?;

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

        // Create a simple hash partition expression
        let partition_by = Some(vec![Expr::Column(Column::Resolved(ResolvedColumn::Basic(
            "ints".into(),
        )))
        .into()]);

        // Create the cache with dummy writers
        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            num_partitions,
            partition_by,
            vec![],
        )?;

        // Create and push some partitions
        let mp = make_dummy_mp(150);
        let mp2 = make_dummy_mp(350);

        cache.push_partitions(vec![mp, mp2]).await?;

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

        let partition_by = Some(vec![Expr::Column(Column::Resolved(ResolvedColumn::Basic(
            "ints".into(),
        )))
        .into()]);

        let cache = InProgressShuffleCache::try_new_with_writers(
            writers,
            num_partitions,
            partition_by,
            vec![],
        )?;

        // 1000 empty partitions
        for _ in 0..1000 {
            cache.push_partitions(vec![make_dummy_mp(0)]).await?;
        }

        let shuffle_cache = cache.close().await?;

        // Even though we pushed empty partitions, we should still files.
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
            num_partitions,
            None, // No partition by expressions
            vec![],
        )?;

        let mut found_failure = false;
        // Technically, we can calculate the max number of iterations before failure, based on number of tasks and channel sizes,
        // but 100 should be good enough based on our testing environment.
        let num_iterations = 100;
        for _ in 0..num_iterations {
            let mp = make_dummy_mp(100);
            if let Err(err) = cache.push_partitions(vec![mp]).await {
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
        let mp = make_dummy_mp(100);
        let result = cache.push_partitions(vec![mp]).await;
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
            num_partitions,
            None, // No partition by expressions
            vec![],
        )?;

        // Create and push a partition
        let mp = make_dummy_mp(100);

        // This should succeed since the failure happens on close
        cache.push_partitions(vec![mp]).await?;

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
        let mp = make_dummy_mp(100);
        let result = cache.push_partitions(vec![mp]).await;
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
