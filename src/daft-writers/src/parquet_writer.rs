use std::{
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_runtime::{get_compute_runtime, RuntimeTask};
use daft_core::prelude::*;
use daft_io::{
    get_io_client, parse_url, IOConfig, S3LikeSource, S3MultipartWriter, S3PartBuffer, SourceType,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use parking_lot::Mutex;
use parquet::{
    arrow::{
        arrow_writer::{compute_leaves, get_column_writers, ArrowColumnChunk, ArrowLeafColumn},
        ArrowSchemaConverter,
    },
    basic::Compression,
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    schema::types::SchemaDescriptor,
};
use tokio::task::spawn_blocking;

use crate::{utils::record_batch_to_partition_path, AsyncFileWriter};

type ParquetColumnWriterHandle = RuntimeTask<DaftResult<ArrowColumnChunk>>;

/// We currently support two kinds of storage backends: FileStorageBackend for local file writes,
/// and S3StorageBackend for writes to S3.
#[async_trait]
trait StorageBackend: Send + Sync + 'static {
    type Writer: Write + Send;

    /// Create the output buffer (buffered file writer, S3 buffer, etc).
    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer>;

    /// Finalize the write operation (close file, await upload to S3, etc).
    async fn finalize(&mut self) -> DaftResult<()>;
}

struct FileStorageBackend {}

impl FileStorageBackend {
    // Buffer potentially small writes for highly compressed columns.
    const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024;
}

#[async_trait]
impl StorageBackend for FileStorageBackend {
    type Writer = BufWriter<std::fs::File>;

    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer> {
        // Create directories if they don't exist.
        if let Some(parent) = filename.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(filename)?;
        Ok(BufWriter::with_capacity(
            Self::DEFAULT_WRITE_BUFFER_SIZE,
            file,
        ))
    }
    async fn finalize(&mut self) -> DaftResult<()> {
        // Nothing needed for finalizing file storage.
        Ok(())
    }
}

struct S3StorageBackend {
    scheme: String,
    io_config: IOConfig,
    s3_client: Option<Arc<S3LikeSource>>,
    s3part_buffer: Option<Arc<Mutex<S3PartBuffer>>>,
    upload_thread: Option<JoinHandle<DaftResult<()>>>,
}

impl S3StorageBackend {
    const S3_MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024; // 8 MB
    const S3_MULTIPART_MAX_CONCURRENT_UPLOADS_PER_OBJECT: usize = 100; // 100 uploads per S3 object

    fn new(scheme: String, io_config: IOConfig) -> Self {
        Self {
            scheme,
            io_config,
            s3_client: None,
            s3part_buffer: None,
            upload_thread: None,
        }
    }
}

/// A Send and Sync wrapper around S3PartBuffer.
pub struct SharedS3PartBuffer {
    inner: Arc<Mutex<S3PartBuffer>>,
}

impl Write for SharedS3PartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().flush()
    }
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
    type Writer = SharedS3PartBuffer;

    async fn create_writer(&mut self, filename: &Path) -> DaftResult<Self::Writer> {
        let filename = filename.to_string_lossy().to_string();
        let part_size = NonZeroUsize::new(Self::S3_MULTIPART_PART_SIZE)
            .expect("S3 multipart part size must be non-zero");
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Create the S3 part buffer that interfaces with parquet-rs.
        let s3part_buffer = Arc::new(Mutex::new(S3PartBuffer::new(part_size, tx)));
        self.s3part_buffer = Some(s3part_buffer.clone());

        if self.s3_client.is_none() {
            // Initialize S3 client if needed.
            let io_config = Arc::new(self.io_config.clone());

            let io_client = get_io_client(true, io_config)?;
            let s3_client = io_client
                .get_source(&format!("s3://{}", filename))
                .await?
                .as_any_arc()
                .downcast()
                .unwrap();
            self.s3_client = Some(s3_client);
        }

        // Spawn background upload thread.
        let s3_client = self
            .s3_client
            .clone()
            .expect("S3 client must be initialized");
        let uri = format!("{}://{}", self.scheme, filename);
        let background_thread_handle = std::thread::spawn(move || -> DaftResult<()> {
            let background_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create background uploader tokio runtime");

            background_rt.block_on(async move {
                // Set up multipart writer.
                let mut s3_multipart_writer = S3MultipartWriter::create(
                    uri,
                    part_size,
                    NonZeroUsize::new(Self::S3_MULTIPART_MAX_CONCURRENT_UPLOADS_PER_OBJECT)
                        .expect("S3 multipart concurrent uploads per object must be non-zero"),
                    s3_client,
                )
                .await
                .expect("Failed to create S3 multipart writer");
                while let Some(part) = rx.recv().await {
                    s3_multipart_writer.write_part(part).await?;
                }
                s3_multipart_writer.shutdown().await?;
                Ok(())
            })
        });

        self.upload_thread = Some(background_thread_handle);

        Ok(SharedS3PartBuffer {
            inner: s3part_buffer,
        })
    }

    async fn finalize(&mut self) -> DaftResult<()> {
        let part_buffer = self
            .s3part_buffer
            .take()
            .expect("S3 part buffer must be initialized for multipart upload");
        let upload_thread = self
            .upload_thread
            .take()
            .expect("Upload thread must be initialized for multipart upload");

        tokio::task::spawn_blocking(move || -> DaftResult<()> {
            // Close the S3PartBuffer, this flushes any remaining data to S3 as the final part.
            part_buffer.lock().shutdown()?;
            let upload_result = upload_thread.join();
            if let Err(err) = upload_result {
                log::error!("Upload thread failed: {:?}", err);
            }
            Ok(())
        })
        .await
        .map_err(|e| DaftError::ParquetError(e.to_string()))??;

        Ok(())
    }
}

/// Helper function that checks if we support native writes given the file format, root directory, and schema.
pub(crate) fn native_parquet_writer_supported(
    file_format: FileFormat,
    root_dir: &str,
    file_schema: &SchemaRef,
    remote_writes_enabled: bool,
) -> DaftResult<bool> {
    // TODO(desmond): Currently we only support native parquet writes.
    if !matches!(file_format, FileFormat::Parquet) {
        return Ok(false);
    }
    let (source_type, _) = parse_url(root_dir)?;
    match source_type {
        SourceType::File => {}
        SourceType::S3 if remote_writes_enabled => {}
        _ => return Ok(false),
    }
    // TODO(desmond): Currently we do not support extension and timestamp types.
    let arrow_schema = match file_schema.to_arrow() {
        Ok(schema)
            if schema
                .fields
                .iter()
                .all(|field| field.data_type().can_convert_to_arrow_rs()) =>
        {
            Arc::new(schema.into())
        }
        _ => return Ok(false),
    };
    let writer_properties = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    Ok(ArrowSchemaConverter::new()
        .with_coerce_types(writer_properties.coerce_types())
        .convert(&arrow_schema)
        .is_ok())
}

pub(crate) fn create_native_parquet_writer(
    root_dir: &str,
    schema: &SchemaRef,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
    io_config: Option<IOConfig>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    // Parse the root directory and add partition values if present.
    let (source_type, root_dir) = parse_url(root_dir)?;
    let filename = build_filename(source_type, root_dir.as_ref(), partition_values, file_idx)?;

    // TODO(desmond): Explore configurations such data page size limit, writer version, etc. Parquet format v2
    // could be interesting but has much less support in the ecosystem (including ourselves).
    let writer_properties = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build(),
    );

    let arrow_schema = Arc::new(schema.to_arrow()?.into());

    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(writer_properties.coerce_types())
        .convert(&arrow_schema)
        .expect("By this point `native_writer_supported` should have been called which would have verified that the schema is convertible");

    match source_type {
        SourceType::File => {
            let storage_backend = FileStorageBackend {};
            Ok(Box::new(ParquetWriter::new(
                filename,
                writer_properties,
                arrow_schema,
                parquet_schema,
                partition_values.cloned(),
                storage_backend,
            )))
        }
        SourceType::S3 => {
            let (scheme, _, _) = daft_io::s3_like::parse_s3_url(root_dir.as_ref())?;
            let io_config = io_config.ok_or_else(|| {
                DaftError::InternalError("IO config is required for S3 writes".to_string())
            })?;
            let storage_backend = S3StorageBackend::new(scheme, io_config);
            Ok(Box::new(ParquetWriter::new(
                filename,
                writer_properties,
                arrow_schema,
                parquet_schema,
                partition_values.cloned(),
                storage_backend,
            )))
        }
        _ => Err(DaftError::InternalError(format!(
            "Unsupported source type for the native writer: {source_type}"
        ))),
    }
}

/// Helper function to build the filename for the parquet file.
fn build_filename(
    source_type: SourceType,
    root_dir: &str,
    partition_values: Option<&RecordBatch>,
    file_idx: usize,
) -> DaftResult<PathBuf> {
    let partition_path = get_partition_path(partition_values)?;
    let filename = generate_parquet_filename(file_idx);

    match source_type {
        SourceType::File => build_local_file_path(root_dir, partition_path, filename),
        SourceType::S3 => build_s3_path(root_dir, partition_path, filename),
        _ => Err(DaftError::ValueError(format!(
            "Unsupported source type: {:?}",
            source_type
        ))),
    }
}

/// Helper function to get the partition path from the record batch.
fn get_partition_path(partition_values: Option<&RecordBatch>) -> DaftResult<PathBuf> {
    match partition_values {
        Some(partition_values) => Ok(record_batch_to_partition_path(partition_values, None)?),
        None => Ok(PathBuf::new()),
    }
}

// Helper function to generate the parquet filename.
fn generate_parquet_filename(file_idx: usize) -> String {
    format!("{}-{}.parquet", uuid::Uuid::new_v4(), file_idx)
}

/// Helper function to build the path to a local file.
fn build_local_file_path(
    root_dir: &str,
    partition_path: PathBuf,
    filename: String,
) -> DaftResult<PathBuf> {
    let root_dir = Path::new(root_dir.trim_start_matches("file://"));
    let dir = root_dir.join(partition_path);
    Ok(dir.join(filename))
}

/// Helper function to build the path to an S3 url.
fn build_s3_path(root_dir: &str, partition_path: PathBuf, filename: String) -> DaftResult<PathBuf> {
    let (_scheme, bucket, key) = daft_io::s3_like::parse_s3_url(root_dir)?;
    let key = Path::new(&key).join(partition_path).join(filename);
    Ok(PathBuf::from(format!("{}/{}", bucket, key.display())))
}

struct ParquetWriter<B: StorageBackend> {
    filename: PathBuf,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    partition_values: Option<RecordBatch>,
    storage_backend: B,
    file_writer: Arc<Mutex<Option<SerializedFileWriter<B::Writer>>>>,
}

impl<B: StorageBackend> ParquetWriter<B> {
    const PATH_FIELD_NAME: &str = "path";

    fn new(
        filename: PathBuf,
        writer_properties: Arc<WriterProperties>,
        arrow_schema: Arc<arrow_schema::Schema>,
        parquet_schema: SchemaDescriptor,
        partition_values: Option<RecordBatch>,
        storage_backend: B,
    ) -> Self {
        Self {
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            partition_values,
            storage_backend,
            file_writer: Arc::new(Mutex::new(None)),
        }
    }

    async fn create_writer(&mut self) -> DaftResult<()> {
        let backend_writer = self.storage_backend.create_writer(&self.filename).await?;
        let file_writer = SerializedFileWriter::new(
            backend_writer,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        self.file_writer = Arc::new(Mutex::new(Some(file_writer)));
        Ok(())
    }

    fn extract_leaf_columns_from_record_batches(
        &self,
        record_batches: &[RecordBatch],
        num_leaf_columns: usize,
    ) -> DaftResult<Vec<Vec<ArrowLeafColumn>>> {
        // Preallocate a vector for each leaf column across all record batches.
        let mut leaf_columns: Vec<Vec<ArrowLeafColumn>> = (0..num_leaf_columns)
            .map(|_| Vec::with_capacity(record_batches.len()))
            .collect();
        // Iterate through each record batch and extract its leaf columns.
        for record_batch in record_batches {
            let arrays = record_batch.get_inner_arrow_arrays();
            let mut leaf_column_slots = leaf_columns.iter_mut();

            for (arr, field) in arrays.zip(&self.arrow_schema.fields) {
                let leaves = compute_leaves(field, &arr.into())
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                for leaf in leaves {
                    match leaf_column_slots.next() {
                        Some(slot) => slot.push(leaf),
                        None => {
                            return Err(DaftError::InternalError(
                                "Mismatch between leaves and column slots".to_string(),
                            ))
                        }
                    }
                }
            }
        }
        Ok(leaf_columns)
    }

    /// Helper function that spawns 1 worker thread per leaf column, dispatches the relevant arrow leaf columns to each
    /// worker, then returns the handles to the workers.
    fn spawn_column_writer_workers(
        &self,
        record_batches: &[RecordBatch],
    ) -> DaftResult<Vec<ParquetColumnWriterHandle>> {
        // Get leaf column writers. For example, a struct<int, int> column produces two leaf column writers.
        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

        // Flatten record batches into per-leaf-column Arrow data chunks.
        let leaf_columns =
            self.extract_leaf_columns_from_record_batches(record_batches, column_writers.len())?;

        let compute_runtime = get_compute_runtime();

        // Spawn one worker per leaf column writer.
        Ok(column_writers
            .into_iter()
            .zip(leaf_columns.into_iter())
            .map(|(mut writer, leaf_column)| {
                compute_runtime.spawn(async move {
                    for chunk in leaf_column {
                        writer
                            .write(&chunk)
                            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                    }
                    writer
                        .close()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))
                })
            })
            .collect())
    }
}

#[async_trait]
impl<B: StorageBackend> AsyncFileWriter for ParquetWriter<B> {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        if self.file_writer.lock().is_none() {
            self.create_writer().await?;
        }
        let starting_bytes_written = self.bytes_written();
        let record_batches = data.get_tables()?;

        // Spawn column writers.
        let column_writer_handles = self.spawn_column_writer_workers(&record_batches)?;

        let row_writer_thread_handle = {
            // Wait for the workers to complete encoding, and append the resulting column chunks to the row group and the file.
            let (tx_chunk, mut rx_chunk) = tokio::sync::mpsc::channel::<ArrowColumnChunk>(1);

            let file_writer_handle = self.file_writer.clone();

            // Spawn a thread to handle the row group writing since it involves blocking writes.
            let row_writer_thread_handle = spawn_blocking(move || -> DaftResult<()> {
                let mut guard = file_writer_handle.lock();
                let mut row_group_writer = guard
                    .as_mut()
                    .expect("File writer should be created by now")
                    .next_row_group()
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                while let Some(chunk) = rx_chunk.blocking_recv() {
                    chunk
                        .append_to_row_group(&mut row_group_writer)
                        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                }

                row_group_writer
                    .close()
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                Ok(())
            });

            for handle in column_writer_handles {
                let chunk = handle.await??;
                tx_chunk
                    .send(chunk)
                    .await
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;
            }

            row_writer_thread_handle
            // tx_chunk is dropped here, which signals the row writer thread to finish.
        };

        row_writer_thread_handle
            .await
            .map_err(|e| DaftError::ParquetError(e.to_string()))??;

        Ok(self.bytes_written() - starting_bytes_written)
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.

        // Our file writer might be backed by an S3 part writer that may block when flushing metadata.
        let file_writer_handle = self.file_writer.clone();
        spawn_blocking(move || -> DaftResult<()> {
            let mut guard = file_writer_handle.lock();
            let _metadata = guard
                .as_mut()
                .expect("File writer should be created by now")
                .finish()
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;

            Ok(())
        })
        .await
        .map_err(|e| DaftError::ParquetError(e.to_string()))??;

        // TODO: We can start encoding the next file while this finalization happens.

        // Let the storage backend handle its finalization. For our S3 backend, this waits for all
        // part uploads to complete.
        self.storage_backend.finalize().await?;

        // Return a recordbatch containing the filename that we wrote to.
        let field = Field::new(Self::PATH_FIELD_NAME, DataType::Utf8);
        let filename_series = Series::from_arrow(
            Arc::new(field.clone()),
            Box::new(arrow2::array::Utf8Array::<i64>::from_slice([&self
                .filename
                .to_string_lossy()])),
        )?;
        let record_batch =
            RecordBatch::new_with_size(Schema::new(vec![field]), vec![filename_series], 1)?;
        let record_batch_with_partition_values =
            if let Some(partition_values) = self.partition_values.take() {
                record_batch.union(&partition_values)?
            } else {
                record_batch
            };
        Ok(Some(record_batch_with_partition_values))
    }

    fn bytes_written(&self) -> usize {
        let bytes_written = match self.file_writer.lock().as_ref() {
            Some(writer) => writer.bytes_written(),
            None => unreachable!("File writer must be created before bytes_written can be called"),
        };
        bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written()]
    }
}
