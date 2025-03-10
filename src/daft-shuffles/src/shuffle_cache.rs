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

enum ShuffleCacheState {
    Open {
        writer_tasks: Vec<RuntimeTask<DaftResult<(Vec<usize>, Vec<String>)>>>,
        writer_senders: Vec<tokio::sync::mpsc::Sender<Arc<MicroPartition>>>,
        schema: Option<SchemaRef>,
    },
    Closed {
        bytes_per_file_per_partition: Vec<Vec<usize>>,
        file_paths_per_partition: Vec<Vec<String>>,
        schema: SchemaRef,
    },
}

pub struct ShuffleCache {
    state: Mutex<ShuffleCacheState>,
    partition_by: Option<Vec<ExprRef>>,
    num_partitions: usize,
}

impl ShuffleCache {
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
            state: Mutex::new(ShuffleCacheState::Open {
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

        let mut state_guard = self.state.lock().await;
        let ShuffleCacheState::Open {
            writer_tasks,
            writer_senders,
            schema,
        } = &mut *state_guard
        else {
            return Err(DaftError::InternalError(
                "ShuffleCache should be in open state when pushing partitions".to_string(),
            ));
        };
        if schema.is_none() {
            *schema = Some(input_partition.schema().clone());
        }

        let send_futures = partitioned
            .into_iter()
            .zip(writer_senders.iter())
            .map(|(partition, sender)| sender.send(partition.into()));
        match futures::future::try_join_all(send_futures).await {
            Ok(_) => (),
            Err(e) => {
                let writer_tasks = std::mem::take(writer_tasks);
                let writer_senders = std::mem::take(writer_senders);
                Self::close_internal(writer_tasks, writer_senders).await?;
                return Err(DaftError::InternalError(format!(
                    "Failed to send partition to writer: {}",
                    e
                )));
            }
        }
        Ok(())
    }

    pub async fn schema(&self) -> Option<SchemaRef> {
        let state_guard = self.state.lock().await;
        let ShuffleCacheState::Closed { schema, .. } = &*state_guard else {
            return None;
        };
        Some(schema.clone())
    }

    pub async fn bytes_per_file(&self, partition_idx: usize) -> DaftResult<Vec<usize>> {
        let state_guard = self.state.lock().await;
        let ShuffleCacheState::Closed {
            bytes_per_file_per_partition,
            ..
        } = &*state_guard
        else {
            return Err(DaftError::InternalError(
                "ShuffleCache should be in closed state when retrieving bytes per file".to_string(),
            ));
        };
        Ok(bytes_per_file_per_partition[partition_idx].clone())
    }

    pub async fn file_paths(&self, partition_idx: usize) -> DaftResult<Vec<String>> {
        let state_guard = self.state.lock().await;
        let ShuffleCacheState::Closed {
            file_paths_per_partition,
            ..
        } = &*state_guard
        else {
            return Err(DaftError::InternalError(
                "ShuffleCache should be in closed state when getting file paths".to_string(),
            ));
        };
        Ok(file_paths_per_partition[partition_idx].clone())
    }

    pub async fn close(&self) -> DaftResult<()> {
        let mut state_guard = self.state.lock().await;
        let ShuffleCacheState::Open {
            writer_tasks,
            writer_senders,
            schema,
        } = &mut *state_guard
        else {
            return Err(DaftError::InternalError(
                "ShuffleCache cannot be closed more than once".to_string(),
            ));
        };

        let writer_tasks = std::mem::take(writer_tasks);
        let writer_senders = std::mem::take(writer_senders);
        let (bytes_per_file_per_partition, file_paths_per_partition): (
            Vec<Vec<usize>>,
            Vec<Vec<String>>,
        ) = Self::close_internal(writer_tasks, writer_senders)
            .await?
            .into_iter()
            .unzip();
        *state_guard = ShuffleCacheState::Closed {
            bytes_per_file_per_partition,
            file_paths_per_partition,
            schema: schema.as_ref().unwrap().clone(),
        };
        Ok(())
    }

    async fn close_internal(
        writer_tasks: Vec<RuntimeTask<DaftResult<(Vec<usize>, Vec<String>)>>>,
        writer_senders: Vec<tokio::sync::mpsc::Sender<Arc<MicroPartition>>>,
    ) -> DaftResult<Vec<(Vec<usize>, Vec<String>)>> {
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
) -> DaftResult<(Vec<usize>, Vec<String>)> {
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
    Ok((bytes_per_file, file_paths))
}
