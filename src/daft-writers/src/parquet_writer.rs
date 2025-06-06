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
use daft_io::{parse_url, IOConfig, S3LikeSource, S3MultipartWriter, S3PartBuffer, SourceType};
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

enum OutputTarget {
    File(BufWriter<std::fs::File>),
    S3(Arc<Mutex<S3PartBuffer>>),
}

impl Write for OutputTarget {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.write(buf),
            Self::S3(part_buffer) => part_buffer.lock().write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(f) => f.flush(),
            Self::S3(part_buffer) => part_buffer.lock().flush(),
        }
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
    let filename = if matches!(source_type, SourceType::File) {
        let root_dir = Path::new(root_dir.trim_start_matches("file://"));
        let dir = if let Some(partition_values) = partition_values {
            let partition_path = record_batch_to_partition_path(partition_values, None)?;
            root_dir.join(partition_path)
        } else {
            root_dir.to_path_buf()
        };
        // Create the directories if they don't exist.
        std::fs::create_dir_all(&dir)?;

        dir.join(format!("{}-{}.parquet", uuid::Uuid::new_v4(), file_idx))
    } else {
        let root = root_dir.trim_start_matches("s3://");
        let (bucket, prefix) = root.split_once('/').unwrap();
        let partition_path = if let Some(partition_values) = partition_values {
            record_batch_to_partition_path(partition_values, None)?
        } else {
            PathBuf::new()
        };
        let key = Path::new(prefix).join(partition_path).join(format!(
            "{}-{}.parquet",
            uuid::Uuid::new_v4(),
            file_idx
        ));

        PathBuf::from(format!("s3://{}/{}", bucket, key.display()))
    };

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

    Ok(Box::new(ParquetWriter::new(
        filename,
        writer_properties,
        arrow_schema,
        parquet_schema,
        partition_values.cloned(),
        io_config,
    )))
}

struct ParquetWriter {
    filename: PathBuf,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    partition_values: Option<RecordBatch>,
    file_writer: Arc<Mutex<Option<SerializedFileWriter<OutputTarget>>>>,
    io_config: Option<IOConfig>,
    s3_client: Option<Arc<S3LikeSource>>,
    s3part_buffer: Option<Arc<Mutex<S3PartBuffer>>>,
    s3multipart_writer: Option<Arc<tokio::sync::Mutex<S3MultipartWriter>>>,
    upload_thread: Option<JoinHandle<DaftResult<()>>>,
}

const S3_MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024; // 8 MB

const S3_MULTIPART_MAX_CONCURRENT_UPLOADS_PER_OBJECT: usize = 100; // 100 uploads per S3 object

impl ParquetWriter {
    /// TODO(desmond): This can be tuned.
    /// Default buffer size for writing to files.
    const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;
    const PATH_FIELD_NAME: &str = "path";

    fn new(
        filename: PathBuf,
        writer_properties: Arc<WriterProperties>,
        arrow_schema: Arc<arrow_schema::Schema>,
        parquet_schema: SchemaDescriptor,
        partition_values: Option<RecordBatch>,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            partition_values,
            file_writer: Arc::new(Mutex::new(None)),
            io_config,
            s3_client: None,
            s3part_buffer: None,
            s3multipart_writer: None,
            upload_thread: None,
        }
    }

    async fn create_writer(&mut self) -> DaftResult<()> {
        let output_target = if self.filename.to_string_lossy().starts_with("s3") {
            let url = self.filename.to_string_lossy().to_string();
            let part_size = NonZeroUsize::new(S3_MULTIPART_PART_SIZE)
                .expect("S3 multipart part size must be non-zero");
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);

            // Create the S3 part buffer that interfaces with parquet-rs.
            let s3part_buffer = Arc::new(Mutex::new(S3PartBuffer::new(
                part_size, // 5MB part size
                tx,
            )));
            self.s3part_buffer = Some(s3part_buffer.clone());

            if self.s3_client.is_none() {
                let s3_conf = &self.io_config.as_ref().unwrap().s3;
                self.s3_client = Some(S3LikeSource::get_client(s3_conf).await?);
            }

            let s3_multipart_writer = Arc::new(tokio::sync::Mutex::new(
                S3MultipartWriter::create(
                    url.clone(),
                    part_size,
                    NonZeroUsize::new(S3_MULTIPART_MAX_CONCURRENT_UPLOADS_PER_OBJECT)
                        .expect("S3 multipart concurrent uploads per object must be non-zero."),
                    self.s3_client
                        .clone()
                        .expect("S3 client must be initialized for multipart upload."),
                )
                .await
                .expect("Failed to create S3 multipart writer"),
            ));

            self.s3multipart_writer = Some(s3_multipart_writer);

            let s3_multipart_writer_handle = self
                .s3multipart_writer
                .clone()
                .expect("S3 multipart writer must be initialized");

            // Spawn a background thread to handle the multipart upload.
            let background_thread_handle = std::thread::spawn(move || -> DaftResult<()> {
                let background_rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create background uploader tokio runtime");

                background_rt.block_on(async move {
                    while let Some(part) = rx.recv().await {
                        // Write the part to S3.
                        s3_multipart_writer_handle
                            .lock()
                            .await
                            .write_part(part)
                            .await?;
                    }

                    s3_multipart_writer_handle.lock().await.shutdown().await?;

                    Ok(())
                })
            });
            self.upload_thread = Some(background_thread_handle);

            OutputTarget::S3(s3part_buffer)
        } else {
            let file = std::fs::File::create(&self.filename)?;
            OutputTarget::File(BufWriter::with_capacity(
                Self::DEFAULT_WRITE_BUFFER_SIZE,
                file,
            ))
        };
        let writer = SerializedFileWriter::new(
            output_target,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        self.file_writer = Arc::new(Mutex::new(Some(writer)));
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
impl AsyncFileWriter for ParquetWriter {
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

        // Important to drop the sender to signal completion to the row writer thread, else
        // awaiting for thread to complete will hang.
        drop(tx_chunk);

        row_writer_thread_handle
            .await
            .map_err(|e| DaftError::ParquetError(e.to_string()))??;

        Ok(self.bytes_written() - starting_bytes_written)
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.

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

        let part_buffer = self
            .s3part_buffer
            .take()
            .expect("S3 part buffer must be initialized for multipart upload.");
        let upload_thread = self
            .upload_thread
            .take()
            .expect("Upload thread must be initialized for multipart upload.");

        spawn_blocking(move || -> DaftResult<()> {
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
        let bytes_written = match self.file_writer.lock().as_ref() {
            Some(writer) => writer.bytes_written(),
            None => unreachable!("File writer must be created before bytes_per_file can be called"),
        };
        vec![bytes_written]
    }
}
