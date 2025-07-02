use std::{path::PathBuf, sync::Arc};

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_json::{writer::LineDelimited, LineDelimitedWriter, WriterBuilder};
use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
use daft_io::{parse_url, IOConfig, SourceType};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    storage_backend::{FileStorageBackend, S3StorageBackend, StorageBackend},
    utils::build_filename,
    AsyncFileWriter,
};

/// Helper function that checks if we support native writes given the file format, root directory, and schema.
pub(crate) fn native_json_writer_supported(file_schema: &SchemaRef) -> DaftResult<bool> {
    // TODO(desmond): Currently we do not support extension and timestamp types.
    let datatypes_convertable = file_schema.to_arrow()?.fields.iter().all(|field| {
        field.data_type().can_convert_to_arrow_rs() && field.data_type().can_convert_to_json()
    });
    Ok(datatypes_convertable)
}

pub(crate) fn create_native_json_writer(
    root_dir: &str,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
    io_config: Option<IOConfig>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    // Parse the root directory and add partition values if present.
    let (source_type, root_dir) = parse_url(root_dir)?;
    let filename = build_filename(
        source_type,
        root_dir.as_ref(),
        partition_values,
        file_idx,
        "json",
    )?;
    match source_type {
        SourceType::File => {
            let storage_backend = FileStorageBackend {};
            Ok(Box::new(JsonWriter::new(
                filename,
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
            Ok(Box::new(JsonWriter::new(
                filename,
                partition_values.cloned(),
                storage_backend,
            )))
        }
        _ => Err(DaftError::ValueError(format!(
            "Unsupported source type: {:?}",
            source_type
        ))),
    }
}

struct JsonWriter<B: StorageBackend> {
    filename: PathBuf,
    partition_values: Option<RecordBatch>,
    storage_backend: B,
    file_writer: Option<LineDelimitedWriter<B::Writer>>,
    bytes_written: usize,
}

impl<B: StorageBackend> JsonWriter<B> {
    const PATH_FIELD_NAME: &str = "path";
    const INFLATION_FACTOR: f64 = 0.5;

    fn new(filename: PathBuf, partition_values: Option<RecordBatch>, storage_backend: B) -> Self {
        Self {
            filename,
            partition_values,
            storage_backend,
            file_writer: None,
            bytes_written: 0,
        }
    }

    /// Estimates the number of bytes that will be written for the given data.
    /// This is a temporary workaround since arrow-json doesn't provide bytes written or access to the underlying writer.
    fn estimate_bytes_to_write(&self, data: &MicroPartition) -> DaftResult<usize> {
        let base_size = data.size_bytes()?.unwrap_or(0);
        let estimated_size = (base_size as f64 * Self::INFLATION_FACTOR) as usize;
        Ok(estimated_size)
    }

    async fn create_writer(&mut self) -> DaftResult<()> {
        let backend_writer = self.storage_backend.create_writer(&self.filename).await?;
        let builder = WriterBuilder::new().with_explicit_nulls(true);
        let file_writer = builder.build::<_, LineDelimited>(backend_writer);
        self.file_writer = Some(file_writer);
        Ok(())
    }
}

#[async_trait]
impl<B: StorageBackend> AsyncFileWriter for JsonWriter<B> {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        if self.file_writer.is_none() {
            self.create_writer().await?;
        }
        // TODO(desmond): This is a hack to estimate the size in bytes. Arrow-json currently doesn't support getting the
        // bytes written, nor does it allow us to access the LineDelimitedWriter's inner writer which prevents
        // us from using a counting writer. We need to fix this upstream.
        let est_bytes_to_write = self.estimate_bytes_to_write(&data)?;
        self.bytes_written += est_bytes_to_write;
        let record_batches = data.get_tables()?;
        let record_batches: Vec<ArrowRecordBatch> = record_batches
            .iter()
            .map(|rb| rb.clone().try_into())
            .collect::<DaftResult<_>>()?;

        let mut file_writer = self
            .file_writer
            .take()
            .expect("File writer should be created by now");
        let io_runtime = get_io_runtime(true);
        let row_group_writer_thread_handle =
            io_runtime.spawn_blocking(move || -> DaftResult<LineDelimitedWriter<_>> {
                let record_batch_refs: Vec<&ArrowRecordBatch> = record_batches.iter().collect();
                file_writer.write_batches(&record_batch_refs)?;
                Ok(file_writer)
            });
        let file_writer = row_group_writer_thread_handle
            .await
            .map_err(|e| DaftError::ParquetError(e.to_string()))??;
        self.file_writer.replace(file_writer);

        Ok(est_bytes_to_write)
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        let io_runtime = get_io_runtime(true);
        let mut file_writer = self.file_writer.take().unwrap();
        self.file_writer = Some(
            io_runtime
                .spawn_blocking(move || -> DaftResult<LineDelimitedWriter<_>> {
                    file_writer.finish()?;

                    Ok(file_writer)
                })
                .await
                .map_err(|e| DaftError::ParquetError(e.to_string()))??,
        );
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
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written()]
    }
}
