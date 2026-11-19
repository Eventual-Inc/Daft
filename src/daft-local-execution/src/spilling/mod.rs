//! Spill-file lifecycle management and shared encoding for Daft partitions.
//!
//! Pipelines share worker-wide I/O concurrency and spill workspace, but own
//! separate execution directories. Files are published only after a successful
//! write and are removed when their last handle is dropped. Blocking file I/O
//! retains both the file handle and its memory reservation until it completes.
//!
//! Each file contains independently readable Arrow IPC frames. Frame size bounds
//! encoding/decoding workspace; file rotation is independent, so small merge
//! output batches can be appended to the same file. Operators own run metadata,
//! choose what to spill, and reserve execution memory before reading frames back.

use std::{
    fs::{self, File},
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use daft_core::prelude::SchemaRef;
use daft_memory::{MemoryManager, MemoryPermit, MemoryPool};
use daft_micropartition::MicroPartition;
use thiserror::Error;
use tokio::sync::Semaphore;
use uuid::Uuid;

const PARTITION_SPILL_MAGIC: &[u8; 8] = b"DAFTSP01";
const MAX_SCHEMA_FRAME_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_SPILL_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

mod stream;
pub(crate) use stream::SpillStreamWriter;
#[cfg(test)]
pub(crate) use stream::TARGET_FILE_BYTES;

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("at least one spill directory is required")]
    NoDirectories,
    #[error("invalid spill path component: {0}")]
    InvalidPathComponent(String),
    #[error("spill I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("spill I/O task failed: {0}")]
    Task(String),
    #[error("spill data conversion failed: {0}")]
    Data(String),
    #[error("spill memory reservation failed: {0}")]
    Memory(String),
}

#[derive(Clone, Debug)]
pub struct SpillScopeId {
    pipeline: String,
    input: String,
    node: String,
    attempt: String,
}

impl SpillScopeId {
    fn new(
        pipeline_id: impl ToString,
        input_id: impl ToString,
        node_id: impl ToString,
        attempt_id: impl ToString,
    ) -> Self {
        Self {
            pipeline: pipeline_id.to_string(),
            input: input_id.to_string(),
            node: node_id.to_string(),
            attempt: attempt_id.to_string(),
        }
    }
}

#[derive(Debug)]
struct ManagerInner {
    execution_directories: Vec<PathBuf>,
    io_runtime: Arc<SpillIoRuntime>,
    pipeline_id: String,
    attempt_id: String,
}

impl Drop for ManagerInner {
    fn drop(&mut self) {
        for directory in &self.execution_directories {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

#[derive(Debug)]
pub(crate) struct SpillIoRuntime {
    roots: Vec<PathBuf>,
    process_id: String,
    next_directory: AtomicUsize,
    io_slots: Arc<Semaphore>,
    memory_pool: Arc<MemoryPool>,
}

impl SpillIoRuntime {
    pub(crate) fn new(
        directories: impl IntoIterator<Item = PathBuf>,
        io_concurrency: usize,
        memory_pool: Arc<MemoryPool>,
    ) -> Result<Arc<Self>, SpillError> {
        let roots: Vec<_> = directories
            .into_iter()
            .filter(|directory| !directory.as_os_str().is_empty())
            .collect();
        if roots.is_empty() {
            return Err(SpillError::NoDirectories);
        }
        Ok(Arc::new(Self {
            roots,
            process_id: format!("{}-{}", std::process::id(), Uuid::new_v4()),
            next_directory: AtomicUsize::new(0),
            io_slots: Arc::new(Semaphore::new(io_concurrency.max(1))),
            memory_pool,
        }))
    }
}

#[derive(Clone, Debug)]
pub struct SpillManager {
    inner: Arc<ManagerInner>,
}

impl SpillManager {
    pub fn new(
        directories: impl IntoIterator<Item = PathBuf>,
        pipeline_id: impl ToString,
    ) -> Result<Self, SpillError> {
        Self::with_io_concurrency(directories, 2, pipeline_id)
    }

    pub fn with_io_concurrency(
        directories: impl IntoIterator<Item = PathBuf>,
        io_concurrency: usize,
        pipeline_id: impl ToString,
    ) -> Result<Self, SpillError> {
        // Standalone callers own a private workspace. Executions use for_execution
        // with the worker's shared, pre-reserved spill capacity instead.
        let memory_pool = MemoryManager::new(DEFAULT_SPILL_MEMORY_BYTES).worker_pool();
        let io_runtime = SpillIoRuntime::new(directories, io_concurrency, memory_pool)?;
        Ok(Self::for_execution(io_runtime, pipeline_id))
    }

    pub(crate) fn for_execution(
        io_runtime: Arc<SpillIoRuntime>,
        pipeline_id: impl ToString,
    ) -> Self {
        let execution_id = Uuid::new_v4().to_string();
        let execution_directories = io_runtime
            .roots
            .iter()
            .map(|root| root.join(&io_runtime.process_id).join(&execution_id))
            .collect();
        Self {
            inner: Arc::new(ManagerInner {
                execution_directories,
                io_runtime,
                pipeline_id: pipeline_id.to_string(),
                attempt_id: Uuid::new_v4().to_string(),
            }),
        }
    }

    #[must_use]
    pub fn scope(&self, input_id: impl ToString, node_id: impl ToString) -> SpillScopeId {
        SpillScopeId::new(
            &self.inner.pipeline_id,
            input_id,
            node_id,
            &self.inner.attempt_id,
        )
    }

    /// Runs encoding and file I/O on the blocking pool. The file becomes visible to readers only
    /// after the callback succeeds and the writer is committed.
    pub async fn write_file<F>(
        &self,
        scope: SpillScopeId,
        write: F,
    ) -> Result<SpillFile, SpillError>
    where
        F: FnOnce(&mut SpillWriter) -> Result<(), SpillError> + Send + 'static,
    {
        let permit = self
            .inner
            .io_runtime
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        let manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut writer = manager.create_file(&scope)?;
            write(&mut writer)?;
            writer.commit()
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?
    }

    /// Runs decoding and file I/O on the blocking pool while retaining the spill file for the
    /// duration of the callback.
    pub async fn read_file<T, F>(&self, file: &SpillFile, read: F) -> Result<T, SpillError>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> Result<T, SpillError> + Send + 'static,
    {
        let permit = self
            .inner
            .io_runtime
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        let file = file.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            read(file.path())
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?
    }

    pub async fn write_micropartitions(
        &self,
        scope: SpillScopeId,
        partitions: Vec<MicroPartition>,
    ) -> Result<SpillFile, SpillError> {
        let Some(first) = partitions.first() else {
            return Err(SpillError::Data(
                "cannot write a spill file without a schema".to_string(),
            ));
        };
        let mut writer = self.partition_writer(scope, first.schema()).await?;
        for partition in partitions {
            writer = writer.append(partition).await?;
        }
        writer.finish().await
    }

    /// Creates an appendable partition file. I/O slots are held only during individual
    /// operations, never while the caller is producing the next batch (which may read spills).
    pub async fn partition_writer(
        &self,
        scope: SpillScopeId,
        schema: SchemaRef,
    ) -> Result<SpillPartitionWriter, SpillError> {
        let manager = self.clone();
        let io_slots = self.inner.io_runtime.io_slots.clone();
        let memory_pool = self.inner.io_runtime.memory_pool.clone();
        let arrow_schema = schema
            .to_arrow()
            .map_err(|error| SpillError::Data(error.to_string()))?;
        // Never hold an I/O slot while waiting for workspace. Readers may need
        // that slot in order for another spill operation to make progress.
        let memory = memory_pool
            .reserve(schema_working_bytes(&arrow_schema))
            .await
            .map_err(|error| SpillError::Memory(error.to_string()))?;
        let permit = io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _memory = memory;
            let mut writer = manager.create_file(&scope)?;
            write_ipc_frame(&mut writer, &arrow_schema, None)?;
            let bytes_written = writer.stream_position()?;
            let schema_bytes = bytes_written - PARTITION_SPILL_MAGIC.len() as u64 - 8;
            if schema_bytes > MAX_SCHEMA_FRAME_BYTES {
                return Err(SpillError::Data(format!(
                    "partition spill schema frame is too large: {schema_bytes} bytes"
                )));
            }
            Ok(SpillPartitionWriter {
                writer,
                schema,
                io_slots,
                memory_pool,
                bytes_written,
                schema_bytes,
                max_frame_bytes: 0,
            })
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?
    }

    pub async fn open_micropartitions<F, R, E>(
        &self,
        file: &SpillFile,
        reserve: F,
    ) -> Result<SpillBatchReader, SpillError>
    where
        F: FnOnce(u64) -> R,
        R: Future<Output = Result<MemoryPermit, E>>,
        E: std::fmt::Display,
    {
        let spill_file = file.clone();
        let io_permit = self
            .inner
            .io_runtime
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        let path = file.path().to_owned();
        let (mut input, schema_len) = tokio::task::spawn_blocking(move || {
            let _io_permit = io_permit;
            let mut input = File::open(path)?;
            let mut magic = [0_u8; PARTITION_SPILL_MAGIC.len()];
            input.read_exact(&mut magic)?;
            if &magic != PARTITION_SPILL_MAGIC {
                return Err(SpillError::Data(
                    "invalid partition spill header".to_string(),
                ));
            }
            let schema_len = read_frame_len(&mut input)?.ok_or_else(|| {
                SpillError::Data("partition spill is missing its schema".to_string())
            })?;
            if schema_len > MAX_SCHEMA_FRAME_BYTES {
                return Err(SpillError::Data(format!(
                    "partition spill schema frame is too large: {schema_len} bytes"
                )));
            }
            Ok::<_, SpillError>((input, schema_len))
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))??;
        let schema_memory = reserve(schema_len)
            .await
            .map_err(|error| SpillError::Memory(error.to_string()))?;
        let io_permit = self
            .inner
            .io_runtime
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        let (input, schema, schema_memory) = tokio::task::spawn_blocking(move || {
            let _io_permit = io_permit;
            let schema = read_ipc_schema(&mut input, schema_len);
            (input, schema, schema_memory)
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?;
        let schema = schema?;
        Ok(SpillBatchReader {
            file: Some(input),
            schema,
            _spill_file: spill_file,
            io_slots: self.inner.io_runtime.io_slots.clone(),
            _schema_memory: schema_memory,
        })
    }

    fn create_file(&self, scope: &SpillScopeId) -> Result<SpillWriter, SpillError> {
        let components = [&scope.pipeline, &scope.input, &scope.node, &scope.attempt];
        for component in components {
            validate_component(component)?;
        }
        let index = self
            .inner
            .io_runtime
            .next_directory
            .fetch_add(1, Ordering::Relaxed)
            % self.inner.execution_directories.len();
        let directory = self.inner.execution_directories[index]
            .join(&scope.pipeline)
            .join(&scope.input)
            .join(&scope.node)
            .join(&scope.attempt);
        fs::create_dir_all(&directory)?;
        let file_id = Uuid::new_v4().to_string();
        let temporary_path = directory.join(format!("{file_id}.tmp"));
        let committed_path = directory.join(format!("{file_id}.spill"));
        let file = File::create(&temporary_path)?;
        Ok(SpillWriter {
            manager: self.inner.clone(),
            file: Some(file),
            temporary_path,
            committed_path,
        })
    }
}

/// Owns an uncommitted file; dropping it, including on cancellation or encoding failure,
/// removes the temporary file. Each append completes before the next batch is requested.
pub struct SpillPartitionWriter {
    writer: SpillWriter,
    schema: SchemaRef,
    io_slots: Arc<Semaphore>,
    memory_pool: Arc<MemoryPool>,
    bytes_written: u64,
    schema_bytes: u64,
    max_frame_bytes: u64,
}

impl SpillPartitionWriter {
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Schema and the largest data frame can be live together. Reserve space for
    /// both the IPC body and its decoded Arrow buffers, not all frames in an append.
    pub(crate) fn read_bytes(&self) -> u64 {
        self.schema_bytes
            .saturating_add(decode_budget(self.max_frame_bytes))
    }

    pub async fn append(mut self, partition: MicroPartition) -> Result<Self, SpillError> {
        if partition.schema() != self.schema {
            return Err(SpillError::Data("spill batch schema mismatch".to_string()));
        }
        for batch in partition.record_batches() {
            let batch = convert_batch(batch.clone(), self.io_slots.clone()).await?;
            let mut start = 0;
            while start < batch.num_rows() {
                let rows = stream::block_rows(
                    &batch,
                    start,
                    stream::TARGET_BLOCK_BYTES,
                    self.memory_pool.limit_bytes(),
                )?;
                self = self.append_arrow(batch.slice(start, rows)).await?;
                start += rows;
            }
        }
        Ok(self)
    }

    async fn append_arrow(mut self, batch: arrow_array::RecordBatch) -> Result<Self, SpillError> {
        let memory = self
            .memory_pool
            .reserve(encoding_working_bytes(&batch)?)
            .await
            .map_err(|error| SpillError::Memory(error.to_string()))?;
        let permit = self
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // The blocking task owns the permit even if its async caller is cancelled.
            let _memory = memory;
            let before = self.writer.stream_position()?;
            write_ipc_frame(&mut self.writer, batch.schema().as_ref(), Some(&batch))?;
            self.bytes_written = self.writer.stream_position()?;
            self.max_frame_bytes = self.max_frame_bytes.max(self.bytes_written - before - 8);
            Ok(self)
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?
    }

    pub async fn finish(mut self) -> Result<SpillFile, SpillError> {
        let permit = self
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            self.writer.write_all(&0_u64.to_le_bytes())?;
            self.writer.commit()
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?
    }
}

pub struct SpillBatchReader {
    file: Option<File>,
    schema: SchemaRef,
    // Retain the handle so the backing file cannot be deleted while it is being read.
    _spill_file: SpillFile,
    io_slots: Arc<Semaphore>,
    _schema_memory: MemoryPermit,
}

impl SpillBatchReader {
    /// Position of the next frame, for closing a reader between output handoffs.
    pub(crate) fn position(&mut self) -> Result<u64, SpillError> {
        Ok(self
            .file
            .as_mut()
            .expect("reader is idle")
            .stream_position()?)
    }

    /// Resume at a frame boundary previously obtained from `position` on this file.
    pub(crate) fn seek_to(&mut self, position: u64) -> Result<(), SpillError> {
        self.file
            .as_mut()
            .expect("reader is idle")
            .seek(SeekFrom::Start(position))?;
        Ok(())
    }

    pub async fn next_batch<F, R, E>(
        mut self,
        reserve: F,
    ) -> Result<Option<(daft_recordbatch::RecordBatch, MemoryPermit, Self)>, SpillError>
    where
        F: FnOnce(u64) -> R,
        R: Future<Output = Result<MemoryPermit, E>>,
        E: std::fmt::Display,
    {
        let io_permit = self
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        let mut file = self
            .file
            .take()
            .ok_or_else(|| SpillError::Task("spill batch reader is already in use".to_string()))?;
        let (file, frame_len) = tokio::task::spawn_blocking(move || {
            let _io_permit = io_permit;
            let frame_len = read_frame_len(&mut file);
            (file, frame_len)
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?;
        self.file = Some(file);
        let Some(frame_len) = frame_len? else {
            return Ok(None);
        };
        // Account for the IPC body and decoded Arrow buffers coexisting during conversion.
        // This is not an upper bound on arbitrary Python objects reconstructed by pickle.
        let decode_budget = decode_budget(frame_len);
        let memory = reserve(decode_budget)
            .await
            .map_err(|error| SpillError::Memory(error.to_string()))?;

        let io_permit = self
            .io_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SpillError::Task(error.to_string()))?;
        let mut file = self.file.take().expect("spill reader file must be present");
        let (reader, batch, memory) = tokio::task::spawn_blocking(move || {
            let _io_permit = io_permit;
            let result = read_ipc_batch(&mut file, frame_len, self.schema.clone());
            // Cancellation of the awaiting future must not release the reservation
            // while this non-cancellable blocking operation is still decoding.
            self.file = Some(file);
            (self, result, memory)
        })
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?;
        self = reader;
        let mut memory = memory;
        let batch = batch?;
        let batch_bytes = batch.size_bytes() as u64;
        if batch_bytes > decode_budget {
            return Err(SpillError::Data(format!(
                "decoded partition spill batch exceeds its decode budget: {batch_bytes} > {decode_budget}"
            )));
        }
        memory.shrink_to(batch_bytes);
        Ok(Some((batch, memory, self)))
    }
}

fn decode_budget(frame_len: u64) -> u64 {
    frame_len.saturating_mul(2)
}

async fn convert_batch(
    batch: daft_recordbatch::RecordBatch,
    io_slots: Arc<Semaphore>,
) -> Result<arrow_array::RecordBatch, SpillError> {
    let permit = io_slots
        .acquire_owned()
        .await
        .map_err(|error| SpillError::Task(error.to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        // Native columns share their Arrow buffers. Python conversion still uses
        // whole-batch pickle and is not covered by the IPC encoding workspace.
        batch
            .try_into()
            .map_err(|error: common_error::DaftError| SpillError::Data(error.to_string()))
    })
    .await
    .map_err(|error| SpillError::Task(error.to_string()))?
}

// IPC builds temporary body/metadata buffers. Allow for their capacity growth,
// offset normalization and alignment, in addition to the selected Arrow data.
// This is a working-set estimate, not an allocator-enforced bound on arbitrary codecs.
const ENCODING_BUFFER_FACTOR: u64 = 4;

fn schema_working_bytes(schema: &arrow_schema::Schema) -> u64 {
    let fields = schema
        .fields()
        .iter()
        .fold(0_u64, |sum, field| sum.saturating_add(field.size() as u64));
    let metadata = schema.metadata().iter().fold(0_u64, |sum, (key, value)| {
        sum.saturating_add(key.len() as u64)
            .saturating_add(value.len() as u64)
    });
    fields
        .saturating_add(metadata)
        .saturating_mul(ENCODING_BUFFER_FACTOR)
        .saturating_add(64 * 1024)
}

fn encoding_working_bytes(batch: &arrow_array::RecordBatch) -> Result<u64, SpillError> {
    let data = batch
        .columns()
        .iter()
        .try_fold(0_u64, |sum, array| {
            crate::memory_size::array_bytes(array.as_ref()).map(|bytes| sum.saturating_add(bytes))
        })
        .map_err(|error| SpillError::Data(error.to_string()))?;
    Ok(data
        .saturating_mul(ENCODING_BUFFER_FACTOR)
        .saturating_add(schema_working_bytes(batch.schema().as_ref())))
}

fn write_ipc_frame(
    writer: &mut SpillWriter,
    schema: &arrow_schema::Schema,
    batch: Option<&arrow_array::RecordBatch>,
) -> Result<(), SpillError> {
    if writer.stream_position()? == 0 {
        writer.write_all(PARTITION_SPILL_MAGIC)?;
    }
    let length_position = writer.stream_position()?;
    writer.write_all(&0_u64.to_le_bytes())?;
    let payload_position = writer.stream_position()?;
    {
        let mut stream = arrow_ipc::writer::StreamWriter::try_new(&mut *writer, schema)
            .map_err(|error| SpillError::Data(error.to_string()))?;
        if let Some(batch) = batch {
            stream
                .write(batch)
                .map_err(|error| SpillError::Data(error.to_string()))?;
        }
        stream
            .finish()
            .map_err(|error| SpillError::Data(error.to_string()))?;
    }
    let end = writer.stream_position()?;
    let length = end - payload_position;
    writer.seek(SeekFrom::Start(length_position))?;
    writer.write_all(&length.to_le_bytes())?;
    writer.seek(SeekFrom::Start(end))?;
    Ok(())
}

fn read_frame_len(reader: &mut impl Read) -> Result<Option<u64>, SpillError> {
    let mut bytes = [0_u8; size_of::<u64>()];
    reader.read_exact(&mut bytes)?;
    let len = u64::from_le_bytes(bytes);
    Ok((len != 0).then_some(len))
}

fn read_ipc_schema(file: &mut File, frame_len: u64) -> Result<SchemaRef, SpillError> {
    let frame_start = file.stream_position()?;
    let reader = arrow_ipc::reader::StreamReader::try_new(file.take(frame_len), None)
        .map_err(|error| SpillError::Data(error.to_string()))?;
    let schema = Arc::new(
        reader
            .schema()
            .as_ref()
            .try_into()
            .map_err(|error: common_error::DaftError| SpillError::Data(error.to_string()))?,
    );
    drop(reader);
    let frame_end = frame_start
        .checked_add(frame_len)
        .ok_or_else(|| SpillError::Data("partition spill frame offset overflow".to_string()))?;
    file.seek(SeekFrom::Start(frame_end))?;
    Ok(schema)
}

fn read_ipc_batch(
    file: &mut File,
    frame_len: u64,
    schema: SchemaRef,
) -> Result<daft_recordbatch::RecordBatch, SpillError> {
    let frame_start = file.stream_position()?;
    let batch = {
        let frame = file.take(frame_len);
        let mut reader = arrow_ipc::reader::StreamReader::try_new(frame, None)
            .map_err(|error| SpillError::Data(error.to_string()))?;
        let batch = reader
            .next()
            .transpose()
            .map_err(|error| SpillError::Data(error.to_string()))?
            .ok_or_else(|| SpillError::Data("partition spill frame is empty".to_string()))?;
        if reader
            .next()
            .transpose()
            .map_err(|error| SpillError::Data(error.to_string()))?
            .is_some()
        {
            return Err(SpillError::Data(
                "partition spill frame contains multiple batches".to_string(),
            ));
        }
        batch
    };
    let frame_end = frame_start
        .checked_add(frame_len)
        .ok_or_else(|| SpillError::Data("partition spill frame offset overflow".to_string()))?;
    file.seek(SeekFrom::Start(frame_end))?;
    daft_recordbatch::RecordBatch::from_arrow(schema, batch.columns().to_vec())
        .map_err(|error| SpillError::Data(error.to_string()))
}

fn validate_component(component: &str) -> Result<(), SpillError> {
    let path = Path::new(component);
    let mut components = path.components();
    let valid = matches!(components.next(), Some(Component::Normal(value)) if value == component)
        && components.next().is_none();
    if component.is_empty() || !valid {
        return Err(SpillError::InvalidPathComponent(component.to_owned()));
    }
    Ok(())
}

#[derive(Debug)]
pub struct SpillWriter {
    manager: Arc<ManagerInner>,
    file: Option<File>,
    temporary_path: PathBuf,
    committed_path: PathBuf,
}

impl SpillWriter {
    fn commit(mut self) -> Result<SpillFile, SpillError> {
        let mut file = self.file.take().expect("spill writer already committed");
        if let Err(error) = file.flush().and_then(|()| file.sync_all()) {
            drop(file);
            let _ = fs::remove_file(&self.temporary_path);
            return Err(error.into());
        }
        drop(file);
        if let Err(error) = fs::rename(&self.temporary_path, &self.committed_path) {
            let _ = fs::remove_file(&self.temporary_path);
            return Err(error.into());
        }
        let len = match fs::metadata(&self.committed_path) {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                let _ = fs::remove_file(&self.committed_path);
                return Err(error.into());
            }
        };
        Ok(SpillFile {
            inner: Arc::new(SpillFileInner {
                _manager: self.manager.clone(),
                path: self.committed_path.clone(),
                len,
            }),
        })
    }
}

impl Write for SpillWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file
            .as_mut()
            .expect("spill writer already committed")
            .write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .expect("spill writer already committed")
            .flush()
    }
}

impl Seek for SpillWriter {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file
            .as_mut()
            .expect("spill writer already committed")
            .seek(position)
    }
}

impl Drop for SpillWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = fs::remove_file(&self.temporary_path);
        }
    }
}

#[derive(Debug)]
struct SpillFileInner {
    _manager: Arc<ManagerInner>,
    path: PathBuf,
    len: u64,
}

impl Drop for SpillFileInner {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Clone, Debug)]
pub struct SpillFile {
    inner: Arc<SpillFileInner>,
}

impl SpillFile {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.inner.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::{DataType, Field, Schema};
    use daft_recordbatch::RecordBatch;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("value", DataType::Int64)]))
    }

    fn partition(value: i64) -> MicroPartition {
        let batch = RecordBatch::from_arrow(
            schema(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![value]))],
        )
        .unwrap();
        MicroPartition::new_loaded(schema(), Arc::new(vec![batch]), None)
    }

    #[tokio::test]
    async fn append_roundtrip_and_cleanup_with_one_io_slot() {
        let root = std::env::temp_dir().join(format!("daft-spill-test-{}", Uuid::new_v4()));
        let manager = SpillManager::with_io_concurrency([root.clone()], 1, "test").unwrap();
        let mut writer = manager
            .partition_writer(manager.scope(0, 0), schema())
            .await
            .unwrap();
        for value in 0..100 {
            writer = writer.append(partition(value)).await.unwrap();
        }
        let file = writer.finish().await.unwrap();
        let path = file.path().to_owned();
        let memory = daft_memory::MemoryManager::new(1024 * 1024);
        let mut reader = manager
            .open_micropartitions(&file, |bytes| memory.reserve(bytes))
            .await
            .unwrap();
        drop(file);
        for value in 0..100 {
            assert!(path.exists());
            let (batch, permit, next) = reader
                .next_batch(|bytes| memory.reserve(bytes))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(batch.get_column(0).i64().unwrap().get(0), Some(value));
            drop(permit);
            reader = next;
        }
        assert!(
            reader
                .next_batch(|bytes| memory.reserve(bytes))
                .await
                .unwrap()
                .is_none()
        );
        assert!(!path.exists());
        assert_eq!(memory.used_bytes(), 0);
        drop(manager);
        // Execution directories are removed by the manager; the shared process root is empty.
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn abandoned_writer_removes_uncommitted_file() {
        let root = std::env::temp_dir().join(format!("daft-spill-test-{}", Uuid::new_v4()));
        let manager = SpillManager::new([root.clone()], "test").unwrap();
        let writer = manager
            .partition_writer(manager.scope(0, 0), schema())
            .await
            .unwrap()
            .append(partition(1))
            .await
            .unwrap();
        let path = writer.writer.temporary_path.clone();
        assert!(path.exists());
        drop(writer);
        assert!(!path.exists());
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelled_encode_wait_releases_workspace_and_temporary_file() {
        let root = std::env::temp_dir().join(format!("daft-spill-test-{}", Uuid::new_v4()));
        let memory = MemoryManager::with_spill_reserve(2 * 1024 * 1024, 1024 * 1024).unwrap();
        let runtime = SpillIoRuntime::new([root.clone()], 1, memory.spill_pool()).unwrap();
        let manager = SpillManager::for_execution(runtime.clone(), "test");
        let writer = manager
            .partition_writer(manager.scope(0, 0), schema())
            .await
            .unwrap();
        let path = writer.writer.temporary_path.clone();
        let arrow: arrow_array::RecordBatch =
            partition(1).record_batches()[0].clone().try_into().unwrap();
        let io = runtime.io_slots.clone().acquire_owned().await.unwrap();
        let mut pending = Box::pin(writer.append_arrow(arrow));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(memory.spill_pool().used_bytes() > 0);
        drop(pending);
        assert_eq!(memory.used_bytes(), 0);
        assert!(!path.exists());
        drop(io);
        drop(manager);
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn oversized_single_row_fails_without_waiting_or_leaking_workspace() {
        let root = std::env::temp_dir().join(format!("daft-spill-test-{}", Uuid::new_v4()));
        let memory = MemoryManager::with_spill_reserve(1024 * 1024, 128 * 1024).unwrap();
        let runtime = SpillIoRuntime::new([root.clone()], 1, memory.spill_pool()).unwrap();
        let manager = SpillManager::for_execution(runtime, "test");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8)]));
        let batch = RecordBatch::from_arrow(
            schema.clone(),
            vec![Arc::new(arrow_array::LargeStringArray::from_iter_values([
                "x".repeat(128 * 1024),
            ]))],
        )
        .unwrap();
        let writer = manager
            .partition_writer(manager.scope(0, 0), schema.clone())
            .await
            .unwrap();
        let path = writer.writer.temporary_path.clone();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            writer.append(MicroPartition::new_loaded(
                schema,
                Arc::new(vec![batch]),
                None,
            )),
        )
        .await
        .expect("impossible workspace requests must not wait");
        assert!(matches!(result, Err(SpillError::Memory(_))));
        assert_eq!(memory.used_bytes(), 0);
        assert!(!path.exists());
        drop(manager);
        std::fs::remove_dir_all(root).unwrap();
    }
}
