use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_runtime, RuntimeTask};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::{make_ipc_writer, FileWriter};
use tokio::sync::Mutex;

struct ShuffleWriterState {
    writer_tasks: Vec<RuntimeTask<DaftResult<Vec<usize>>>>,
    writer_senders: Vec<tokio::sync::mpsc::Sender<Arc<MicroPartition>>>,
}

pub struct ShuffleWriter {
    state: Mutex<ShuffleWriterState>,
    partition_by: Option<Vec<ExprRef>>,
    num_partitions: usize,
}

impl ShuffleWriter {
    pub fn try_new(
        num_partitions: usize,
        dir: &str,
        target_filesize: usize,
        compression: Option<&str>,
        partition_by: Option<Vec<ExprRef>>,
    ) -> DaftResult<Self> {
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
            state: Mutex::new(ShuffleWriterState {
                writer_tasks,
                writer_senders,
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

    pub async fn close(&self) -> DaftResult<Vec<Vec<usize>>> {
        let mut state = self.state.lock().await;
        let writer_tasks = std::mem::take(&mut state.writer_tasks);
        let writer_senders = std::mem::take(&mut state.writer_senders);
        Self::close_internal(writer_tasks, writer_senders).await
    }

    async fn close_internal(
        writer_tasks: Vec<RuntimeTask<DaftResult<Vec<usize>>>>,
        mut writer_senders: Vec<tokio::sync::mpsc::Sender<Arc<MicroPartition>>>,
    ) -> DaftResult<Vec<Vec<usize>>> {
        writer_senders.clear();
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
) -> DaftResult<Vec<usize>> {
    while let Some(partition) = rx.recv().await {
        writer.write(partition)?;
    }
    writer.close()?;
    Ok(writer.bytes_per_file())
}
