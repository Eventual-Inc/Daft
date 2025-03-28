use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_runtime, get_local_thread_runtime, RuntimeTask};
use daft_dsl::ExprRef;
use daft_io::{parse_url, SourceType};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_writers::{make_ipc_writer, FileWriter};
use tokio::sync::Mutex;

type WriterTaskResult = (Option<SchemaRef>, Vec<(usize, String)>);
type WriterTask = RuntimeTask<DaftResult<WriterTaskResult>>;
struct InProgressShuffleCacheState {
    partitioner_sender: Option<async_channel::Sender<Arc<MicroPartition>>>,
    partitioner_tasks: Vec<RuntimeTask<DaftResult<()>>>,
    writer_tasks: Vec<WriterTask>,
}

pub struct InProgressShuffleCache {
    state: Mutex<InProgressShuffleCacheState>,
    partitioner_sender_weak: async_channel::WeakSender<Arc<MicroPartition>>,
}

impl InProgressShuffleCache {
    pub fn try_new(
        num_partitions: usize,
        dirs: &[String],
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<ExprRef>>,
    ) -> DaftResult<Self> {
        let num_cpus = std::thread::available_parallelism().unwrap().get();
        let local_thread_runtime = get_local_thread_runtime();

        // Create the directories
        for dir in dirs {
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
        let mut partition_writers = Vec::with_capacity(num_partitions);
        for partition_idx in 0..num_partitions {
            let dir = &dirs[partition_idx % dirs.len()];
            let partition_dir = format!("{}/partition_{}", dir, partition_idx);
            std::fs::create_dir_all(&partition_dir)?;

            let partition_writer = make_ipc_writer(&partition_dir, target_filesize, compression)?;
            partition_writers.push(partition_writer);
        }

        // Spawn the writer tasks
        let (writer_tasks, writer_senders): (Vec<_>, Vec<_>) = partition_writers
            .into_iter()
            .map(|writer| {
                let (tx, rx) = async_channel::bounded(num_cpus);
                let task = local_thread_runtime.spawn(async move { writer_task(rx, writer).await });
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
                local_thread_runtime.spawn(async move {
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
            }),
            partitioner_sender_weak: weak_partitioner_sender,
        })
    }

    pub async fn push_partitions(
        &self,
        input_partitions: Vec<Arc<MicroPartition>>,
    ) -> DaftResult<()> {
        let mut should_close = false;
        if let Some(partitioner_sender) = &self.partitioner_sender_weak.upgrade() {
            let send_futures = input_partitions
                .into_iter()
                .map(|partition| partitioner_sender.send(partition));
            if futures::future::try_join_all(send_futures).await.is_err() {
                should_close = true;
            }
        }
        if should_close {
            self.close().await?;
        }
        Ok(())
    }

    pub async fn close(&self) -> DaftResult<ShuffleCache> {
        let mut state = self.state.lock().await;
        let partitioner_sender = state.partitioner_sender.take().unwrap();
        let partitioner_tasks = std::mem::take(&mut state.partitioner_tasks);
        let writer_tasks = std::mem::take(&mut state.writer_tasks);
        let (schemas, writer_results): (Vec<Option<SchemaRef>>, Vec<Vec<(usize, String)>>) =
            Self::close_internal(partitioner_tasks, partitioner_sender, writer_tasks)
                .await?
                .into_iter()
                .unzip();

        let schema = schemas
            .into_iter()
            .find_map(|schema| schema)
            .unwrap_or_else(|| {
                panic!("No schema found in shuffle cache, this should never happen")
            });

        let bytes_per_file_per_partition = writer_results
            .iter()
            .map(|partition| partition.iter().map(|(bytes, _)| *bytes).collect())
            .collect();
        let file_paths_per_partition = writer_results
            .iter()
            .map(|partition| partition.iter().map(|(_, path)| path.clone()).collect())
            .collect();
        Ok(ShuffleCache::new(
            schema,
            bytes_per_file_per_partition,
            file_paths_per_partition,
        ))
    }

    async fn close_internal(
        partitioner_tasks: Vec<RuntimeTask<DaftResult<()>>>,
        partitioner_sender: async_channel::Sender<Arc<MicroPartition>>,
        writer_tasks: Vec<WriterTask>,
    ) -> DaftResult<Vec<WriterTaskResult>> {
        drop(partitioner_sender);
        futures::future::try_join_all(partitioner_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<()>>()?;
        let results = futures::future::try_join_all(writer_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(results)
    }
}

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
                        partition.partition_by_hash(partition_by, num_partitions)?
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

async fn writer_task(
    rx: async_channel::Receiver<Arc<MicroPartition>>,
    mut writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
) -> DaftResult<WriterTaskResult> {
    let compute_runtime = get_compute_runtime();
    let mut schema = None;
    while let Ok(partition) = rx.recv().await {
        if schema.is_none() {
            schema = Some(partition.schema().clone());
        }
        writer = compute_runtime
            .spawn(async move {
                writer.write(partition)?;
                DaftResult::Ok(writer)
            })
            .await??;
    }
    let file_path_tables = writer.close()?;

    let file_paths = file_path_tables
        .into_iter()
        .map(|file_path_table| {
            assert!(file_path_table.num_columns() == 1);
            assert!(file_path_table.num_rows() == 1);
            let path = file_path_table
                .get_column("path")?
                .utf8()?
                .get(0)
                .expect("path column should have one path");
            Ok(path.to_string())
        })
        .collect::<DaftResult<Vec<String>>>()?;

    let bytes_per_file = writer.bytes_per_file();
    assert!(bytes_per_file.len() == file_paths.len());
    Ok((schema, bytes_per_file.into_iter().zip(file_paths).collect()))
}

pub struct ShuffleCache {
    schema: SchemaRef,
    bytes_per_file_per_partition: Vec<Vec<usize>>,
    file_paths_per_partition: Vec<Vec<String>>,
}

impl ShuffleCache {
    pub fn new(
        schema: SchemaRef,
        bytes_per_file_per_partition: Vec<Vec<usize>>,
        file_paths_per_partition: Vec<Vec<String>>,
    ) -> Self {
        Self {
            schema,
            bytes_per_file_per_partition,
            file_paths_per_partition,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn bytes_per_file(&self, partition_idx: usize) -> Vec<usize> {
        self.bytes_per_file_per_partition[partition_idx].clone()
    }

    pub fn file_paths(&self, partition_idx: usize) -> Vec<String> {
        self.file_paths_per_partition[partition_idx].clone()
    }
}
