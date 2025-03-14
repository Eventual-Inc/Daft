use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_runtime, RuntimeTask};
use daft_dsl::ExprRef;
use daft_io::{parse_url, SourceType};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_writers::{make_ipc_writer, FileWriter};
use tokio::sync::Mutex;

type WriterTaskResult = Vec<(usize, String)>;
type WriterTask = RuntimeTask<DaftResult<WriterTaskResult>>;

struct InProgressShuffleCacheState {
    writer_tasks: Vec<WriterTask>,
    writer_senders: Vec<tokio::sync::mpsc::Sender<Arc<MicroPartition>>>,
    schema: Option<SchemaRef>,
}

pub struct InProgressShuffleCache {
    state: Mutex<InProgressShuffleCacheState>,
    partition_by: Option<Vec<ExprRef>>,
    num_partitions: usize,
}

impl InProgressShuffleCache {
    pub fn try_new(
        num_partitions: usize,
        dir: &str,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<ExprRef>>,
    ) -> DaftResult<Self> {
        let (source_type, _) = parse_url(dir)?;
        if source_type != SourceType::File {
            return Err(DaftError::ValueError(format!(
                "ShuffleCache only supports file paths, got: {}",
                dir
            )));
        }

        let partition_writers = (0..num_partitions)
            .map(|partition_idx| make_ipc_writer(dir, partition_idx, target_filesize, compression))
            .collect::<DaftResult<Vec<_>>>()?;

        let mut writer_tasks = Vec::with_capacity(num_partitions);
        let mut writer_senders = Vec::with_capacity(num_partitions);
        for writer in partition_writers {
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            let task = get_compute_runtime().spawn(async move { writer_task(rx, writer).await });
            writer_tasks.push(task);
            writer_senders.push(tx);
        }

        Ok(Self {
            state: Mutex::new(InProgressShuffleCacheState {
                writer_tasks,
                writer_senders,
                schema: None,
            }),
            partition_by,
            num_partitions,
        })
    }

    pub async fn push_partition(&self, input_partition: Arc<MicroPartition>) -> DaftResult<()> {
        let partitioned = match &self.partition_by {
            Some(partition_by) => {
                input_partition.partition_by_hash(partition_by, self.num_partitions)?
            }
            None => input_partition.partition_by_random(self.num_partitions, 0)?,
        };

        let mut state = self.state.lock().await;
        if state.schema.is_none() {
            state.schema = Some(input_partition.schema());
        }

        let send_futures = partitioned
            .into_iter()
            .zip(state.writer_senders.iter())
            .map(|(partition, sender)| sender.send(partition.into()));
        match futures::future::try_join_all(send_futures).await {
            Ok(_) => (),
            Err(e) => {
                let writer_tasks = std::mem::take(&mut state.writer_tasks);
                let writer_senders = std::mem::take(&mut state.writer_senders);
                Self::close_internal(writer_tasks, writer_senders).await?;
                return Err(DaftError::InternalError(format!(
                    "Failed to send partition to writer: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> DaftResult<ShuffleCache> {
        let mut state = self.state.lock().await;
        let writer_tasks = std::mem::take(&mut state.writer_tasks);
        let writer_senders = std::mem::take(&mut state.writer_senders);
        let writer_results = Self::close_internal(writer_tasks, writer_senders).await?;

        let bytes_per_file_per_partition = writer_results
            .iter()
            .map(|partition| partition.iter().map(|(bytes, _)| *bytes).collect())
            .collect();
        let file_paths_per_partition = writer_results
            .iter()
            .map(|partition| partition.iter().map(|(_, path)| path.clone()).collect())
            .collect();
        Ok(ShuffleCache::new(
            state.schema.take().unwrap(),
            bytes_per_file_per_partition,
            file_paths_per_partition,
        ))
    }

    async fn close_internal(
        writer_tasks: Vec<WriterTask>,
        writer_senders: Vec<tokio::sync::mpsc::Sender<Arc<MicroPartition>>>,
    ) -> DaftResult<Vec<WriterTaskResult>> {
        drop(writer_senders);
        let results = futures::future::try_join_all(writer_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(results)
    }
}

async fn writer_task(
    mut rx: tokio::sync::mpsc::Receiver<Arc<MicroPartition>>,
    mut writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
) -> DaftResult<WriterTaskResult> {
    while let Some(partition) = rx.recv().await {
        writer.write(partition)?;
    }
    let file_path_tables = writer.close()?;

    let mut file_paths = Vec::with_capacity(file_path_tables.len());
    for file_path_table in file_path_tables {
        assert!(file_path_table.num_columns() == 1);
        assert!(file_path_table.num_rows() == 1);
        let path = file_path_table
            .get_column("path")?
            .utf8()?
            .get(0)
            .expect("path column should have one path");
        file_paths.push(path.to_string());
    }

    let bytes_per_file = writer.bytes_per_file();
    assert!(bytes_per_file.len() == file_paths.len());
    Ok(bytes_per_file.into_iter().zip(file_paths).collect())
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
