use std::{
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_runtime::{get_compute_runtime, Runtime, RuntimeTask};
use daft_core::prelude::*;
use daft_io::{parse_url, SourceType};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
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
use tokio::sync::mpsc::Sender;

use crate::AsyncFileWriter;

/// TODO(desmond): This can be tuned.
/// Default buffer size for writing to files.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;

type ParquetColumnWriterHandle = RuntimeTask<DaftResult<ArrowColumnChunk>>;

pub(crate) struct NativeArrowWriter {}

impl NativeArrowWriter {
    /// Helper function that checks if we support native writes given the file format, root directory, and schema.
    pub(crate) fn native_supported(
        file_format: FileFormat,
        root_dir: &str,
        file_schema: &SchemaRef,
    ) -> bool {
        // TODO(desmond): Currently we only support native parquet writes.
        if !matches!(file_format, FileFormat::Parquet) {
            return false;
        }
        // TODO(desmond): Currently we only support local writes.
        let (source_type, _) = match parse_url(root_dir) {
            Ok(result) => result,
            Err(_) => return false,
        };
        if !matches!(source_type, SourceType::File) {
            return false;
        }
        // TODO(desmond): Currently we do not extension and timestamp types.
        // Conversion from daft -> arrow2 -> arrow-rs -> parquet also doesn't work for maps.
        // Arrow-rs also does not handle a bug we identified with nested fields that span multiple data pages.
        let writer_properties = Arc::new(
            WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let arrow_schema = match file_schema.to_arrow() {
            Ok(schema) => {
                for field in &schema.fields {
                    if field.data_type().has_non_arrow_rs_convertible_type() {
                        return false;
                    }
                }
                Arc::new(schema.into())
            }
            Err(_) => return false,
        };
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(writer_properties.coerce_types())
            .convert(&arrow_schema);
        parquet_schema.is_ok()
    }

    pub(crate) fn create_parquet_writer(
        root_dir: &str,
        schema: &SchemaRef,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<
        Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    > {
        // Parse the root directory and add partition values if present.
        let (source_type, root_dir) = parse_url(root_dir)?;
        debug_assert!(
            matches!(source_type, SourceType::File),
            "Native writes are currently enabled for local writes only"
        );
        let root_dir = Path::new(root_dir.trim_start_matches("file://"));
        let dir = if let Some(partition_values) = partition_values {
            let partition_path = partition_values.to_partition_path(None)?;
            root_dir.join(partition_path)
        } else {
            root_dir.to_path_buf()
        };
        // Create the directories if they don't exist.
        std::fs::create_dir_all(&dir)?;

        let filename = dir.join(format!("{}-{}.parquet", uuid::Uuid::new_v4(), file_idx));

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
            .expect("By this point we should have verified that the schema is convertible");

        Ok(Box::new(ArrowParquetWriter::new(
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            partition_values.cloned(),
        )))
    }
}

struct ArrowParquetWriter {
    filename: PathBuf,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    partition_values: Option<RecordBatch>,
    file_writer: Option<SerializedFileWriter<BufWriter<std::fs::File>>>,
    compute_runtime: Option<Arc<Runtime>>,
}

impl ArrowParquetWriter {
    fn new(
        filename: PathBuf,
        writer_properties: Arc<WriterProperties>,
        arrow_schema: Arc<arrow_schema::Schema>,
        parquet_schema: SchemaDescriptor,
        partition_values: Option<RecordBatch>,
    ) -> Self {
        Self {
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            partition_values,
            file_writer: None,
            compute_runtime: None,
        }
    }

    fn create_writer(&mut self) -> DaftResult<()> {
        let file = std::fs::File::create(&self.filename)?;
        let bufwriter = BufWriter::with_capacity(DEFAULT_WRITE_BUFFER_SIZE, file);
        let writer = SerializedFileWriter::new(
            bufwriter,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        self.file_writer = Some(writer);
        self.compute_runtime = Some(get_compute_runtime());
        Ok(())
    }

    /// Helper function that spawns 1 worker thread per leaf column, returning the handles to the
    /// workers and senders to give each worker arrow leaf columns to write.
    fn spawn_column_writer_workers(
        &self,
        channel_size: usize,
    ) -> DaftResult<(Vec<ColumnWriterHandle>, Vec<Sender<ArrowLeafColumn>>)> {
        let compute_runtime = self
            .compute_runtime
            .as_ref()
            .expect("Compute runtime should be created by now");

        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        let (handles, senders): (Vec<_>, Vec<_>) = column_writers
            .into_iter()
            .map(|mut writer| {
                let (send, mut recv) = tokio::sync::mpsc::channel(channel_size);
                let handle = compute_runtime.spawn(async move {
                    while let Some(col) = recv.recv().await {
                        writer
                            .write(&col)
                            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                    }
                    writer
                        .close()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))
                });
                (handle, send)
            })
            .unzip();

        Ok((handles, senders))
    }

    /// Helper function that dispatches record batches to the worker threads.
    async fn dispatch_record_batches_to_workers(
        &self,
        record_batches: &[RecordBatch],
        mut senders: Vec<Sender<ArrowLeafColumn>>,
    ) -> DaftResult<()> {
        for recordbatch in record_batches {
            let arrays = recordbatch.get_inner_arrow_arrays();
            for ((arr, field), sender) in arrays
                .zip(&self.arrow_schema.fields)
                .zip(senders.iter_mut())
            {
                let leaves = compute_leaves(field, &arr.into())
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                for leaf in leaves {
                    sender.send(leaf).await.map_err(|_| {
                        DaftError::ComputeError("Failed to send column to worker".to_string())
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncFileWriter for ArrowParquetWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        if self.file_writer.is_none() {
            self.create_writer()?;
        }
        let starting_bytes_written = self.bytes_written();
        let record_batches = data.get_tables()?;

        // Initialize column writers and channels.
        let (column_writer_handles, column_writer_senders) =
            self.spawn_column_writer_workers(record_batches.len())?;

        // Send the record batches to the column writer worker threads.
        self.dispatch_record_batches_to_workers(&record_batches, column_writer_senders)
            .await?;

        // Wait for the workers to complete encoding, and append the resulting column chunks to the row group and the file.
        let mut row_group_writer = self
            .file_writer
            .as_mut()
            .expect("File writer should be created by now")
            .next_row_group()
            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        for handle in column_writer_handles {
            let chunk = handle.await??;
            chunk
                .append_to_row_group(&mut row_group_writer)
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        }

        // Close the current row group.
        row_group_writer
            .close()
            .map_err(|e| DaftError::ParquetError(e.to_string()))?;

        Ok(self.bytes_written() - starting_bytes_written)
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.
        let _metadata = self
            .file_writer
            .as_mut()
            .expect("File writer should be created by now")
            .finish()
            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        // Return a recordbatch containing the filename that we wrote to.
        let field = Field::new("path", DataType::Utf8);
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
        self.file_writer
            .as_ref()
            .expect("File writer should be created by now")
            .bytes_written()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        let bytes_written = self
            .file_writer
            .as_ref()
            .expect("File writer should be created by now")
            .bytes_written();
        vec![bytes_written]
    }
}
