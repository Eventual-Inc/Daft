use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_io::{SourceType, parse_url};
use daft_micropartition::MicroPartition;
use daft_schema::schema::SchemaRef;
use daft_writers::{AsyncFileWriter, make_ipc_writer};
use tokio::sync::Mutex;

const SINGLE_FILE_TARGET_SIZE: usize = usize::MAX;

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

struct WriterTaskResult {
    schema: Option<SchemaRef>,
    file_path: Option<String>,
    total_rows_written: usize,
    total_bytes_written: usize,
}

type WriterTask = RuntimeTask<DaftResult<WriterTaskResult>>;

struct InProgressFlightPartitionStoreState {
    writer_senders: Option<Vec<async_channel::Sender<MicroPartition>>>,
    writer_tasks: Vec<WriterTask>,
    error: Option<String>,
}

pub struct InProgressFlightPartitionStore {
    state: Mutex<InProgressFlightPartitionStoreState>,
    writer_senders_weak: Vec<async_channel::WeakSender<MicroPartition>>,
    expected_schema: SchemaRef,
    partition_ref_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct RegisteredFlightPartition {
    pub partition_ref_id: u64,
    pub schema: SchemaRef,
    pub file_path: Option<String>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl RegisteredFlightPartition {
    pub fn has_data(&self) -> bool {
        self.file_path.is_some() && self.num_rows > 0 && self.size_bytes > 0
    }
}

impl InProgressFlightPartitionStore {
    pub fn try_new(
        partition_ref_ids: Vec<u64>,
        dirs: &[String],
        shuffle_id: u64,
        expected_schema: SchemaRef,
        compression: Option<&str>,
    ) -> DaftResult<Self> {
        let shuffle_dirs = get_shuffle_dirs(dirs, shuffle_id);
        for dir in &shuffle_dirs {
            let (source_type, _) = parse_url(dir)?;
            if source_type != SourceType::File {
                return Err(DaftError::ValueError(format!(
                    "Flight partition store only supports file paths, got: {}",
                    dir
                )));
            }

            std::fs::create_dir_all(dir)?;
        }

        let mut writers = Vec::with_capacity(partition_ref_ids.len());
        for partition_ref_id in &partition_ref_ids {
            let partition_dir = get_partition_dir(&shuffle_dirs, *partition_ref_id);
            if std::path::Path::new(&partition_dir).exists() {
                std::fs::remove_dir_all(&partition_dir)?;
            }
            std::fs::create_dir_all(&partition_dir)?;
            let writer = make_ipc_writer(&partition_dir, SINGLE_FILE_TARGET_SIZE, compression)?;
            writers.push(writer);
        }

        let num_cpus = std::thread::available_parallelism().unwrap().get();
        let (writer_tasks, writer_senders): (Vec<_>, Vec<_>) = writers
            .into_iter()
            .map(|writer| {
                let (tx, rx) = async_channel::bounded(num_cpus * 2);
                let task = get_io_runtime(true).spawn(async move { writer_task(rx, writer).await });
                (task, tx)
            })
            .unzip();

        let weak_senders = writer_senders
            .iter()
            .map(|sender| sender.downgrade())
            .collect();

        Ok(Self {
            state: Mutex::new(InProgressFlightPartitionStoreState {
                writer_senders: Some(writer_senders),
                writer_tasks,
                error: None,
            }),
            writer_senders_weak: weak_senders,
            expected_schema,
            partition_ref_ids,
        })
    }

    pub async fn push_partitioned_data(
        &self,
        partitioned_data: Vec<MicroPartition>,
    ) -> DaftResult<()> {
        if partitioned_data.len() != self.writer_senders_weak.len() {
            return Err(DaftError::ValueError(format!(
                "Expected {} partitions in flight partition store, got {}",
                self.writer_senders_weak.len(),
                partitioned_data.len()
            )));
        }

        let send_futures = partitioned_data
            .into_iter()
            .zip(self.writer_senders_weak.iter())
            .map(|(partition, weak_sender)| async move {
                match weak_sender.upgrade() {
                    Some(sender) => sender.send(partition).await.map_err(|e| e.to_string()),
                    None => Err("Flight partition store has been closed".to_string()),
                }
            });

        if let Err(e) = futures::future::try_join_all(send_futures).await {
            self.close().await?;
            return Err(DaftError::InternalError(e));
        }

        Ok(())
    }

    pub async fn close(&self) -> DaftResult<Vec<RegisteredFlightPartition>> {
        let mut state = self.state.lock().await;
        if let Some(error) = &state.error {
            return Err(DaftError::InternalError(error.clone()));
        }

        let writer_senders = state
            .writer_senders
            .take()
            .expect("writer_senders should be present");
        let writer_tasks = std::mem::take(&mut state.writer_tasks);

        let close_result = Self::close_internal(writer_senders, writer_tasks).await;
        if let Err(err) = close_result {
            state.error = Some(err.to_string());
            return Err(err);
        }

        Ok(close_result
            .unwrap()
            .into_iter()
            .zip(self.partition_ref_ids.iter().copied())
            .map(|(result, partition_ref_id)| RegisteredFlightPartition {
                partition_ref_id,
                schema: result
                    .schema
                    .unwrap_or_else(|| self.expected_schema.clone()),
                file_path: result.file_path,
                num_rows: result.total_rows_written,
                size_bytes: result.total_bytes_written,
            })
            .collect())
    }

    async fn close_internal(
        writer_senders: Vec<async_channel::Sender<MicroPartition>>,
        writer_tasks: Vec<WriterTask>,
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
    rx: async_channel::Receiver<MicroPartition>,
    mut writer: Box<
        dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<daft_recordbatch::RecordBatch>>,
    >,
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
    let file_path = match file_path_tables.as_slice() {
        [] => None,
        [file_path_table] => {
            let path = file_path_table
                .get_column(0)
                .utf8()?
                .get(0)
                .expect("path column should have one path");
            Some(path.to_string())
        }
        _ => {
            return Err(DaftError::InternalError(
                "Expected exactly one file path for flight partition".to_string(),
            ));
        }
    };

    Ok(WriterTaskResult {
        schema,
        file_path,
        total_rows_written,
        total_bytes_written,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use daft_core::{
        prelude::{DataType, Field, Int64Array, Schema},
        series::IntoSeries,
    };
    use daft_recordbatch::RecordBatch;

    use super::*;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ints", DataType::Int64)]))
    }

    fn make_mp(values: &[i64]) -> MicroPartition {
        let rb = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_values("ints", values.iter().copied()).into_series(),
        ])
        .unwrap();
        MicroPartition::new_loaded(make_schema(), Arc::new(vec![rb]), None)
    }

    fn make_temp_dir() -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("daft-flight-partition-store-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn test_flight_partition_set_basic() -> DaftResult<()> {
        let temp_dir = make_temp_dir();
        let partitions = InProgressFlightPartitionStore::try_new(
            vec![partition_ref_id(7, 0), partition_ref_id(7, 1)],
            std::slice::from_ref(&temp_dir),
            9,
            make_schema(),
            None,
        )?;

        partitions
            .push_partitioned_data(vec![make_mp(&[1, 2]), make_mp(&[3])])
            .await?;
        partitions
            .push_partitioned_data(vec![make_mp(&[4]), make_mp(&[])])
            .await?;

        let finalized = partitions.close().await?;
        assert_eq!(finalized.len(), 2);
        assert_eq!(finalized[0].partition_ref_id, ((7_u64) << 32));
        assert_eq!(finalized[0].num_rows, 3);
        assert!(finalized[0].file_path.is_some());
        assert_eq!(finalized[1].num_rows, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_flight_partition_set_empty_finalize_uses_expected_schema() -> DaftResult<()> {
        let temp_dir = make_temp_dir();
        let partitions = InProgressFlightPartitionStore::try_new(
            vec![partition_ref_id(1, 0)],
            std::slice::from_ref(&temp_dir),
            2,
            make_schema(),
            None,
        )?;

        let finalized = partitions.close().await?;
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].schema.names(), vec!["ints"]);
        assert!(finalized[0].file_path.is_none());
        assert_eq!(finalized[0].num_rows, 0);

        Ok(())
    }
}
