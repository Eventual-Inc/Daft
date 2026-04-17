use common_error::{DaftError, DaftResult};
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
    #[allow(unused)]
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
    partition_ref_id: u64,
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
        let num_cpus = std::thread::available_parallelism().unwrap().get();
        let (tx, rx) = async_channel::bounded(num_cpus * 2);
        let task = get_io_runtime(true).spawn(async move { writer_task(rx, writer).await });

        Ok(Self {
            state: Mutex::new(InProgressShuffleCacheState {
                writer_sender: Some(tx),
                writer_task: Some(task),
                error: None,
            }),
            partition_ref_id,
        })
    }

    /// Push single partition data to the writer.
    pub async fn push_data(&self, partition: MicroPartition) -> DaftResult<()> {
        let sender = {
            let state = self.state.lock().await;
            if let Some(error) = &state.error {
                return Err(DaftError::InternalError(error.clone()));
            }
            state.writer_sender.clone().ok_or_else(|| {
                DaftError::InternalError("Shuffle cache has been closed".to_string())
            })?
        };

        if let Err(e) = sender.send(partition).await {
            self.close().await?;
            return Err(DaftError::InternalError(e.to_string()));
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

#[derive(Debug, Clone)]
pub struct PartitionCache {
    pub partition_ref_id: u64,
    pub schema: SchemaRef,
    pub file_paths: Vec<String>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl PartitionCache {
    pub fn has_data(&self) -> bool {
        !self.file_paths.is_empty() && self.num_rows > 0 && self.size_bytes > 0
    }
}
