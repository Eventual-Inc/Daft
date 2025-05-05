use std::{
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_runtime::{get_io_runtime, Runtime};
use daft_core::{prelude::*, series::Series};
use daft_io::{parse_url, IOStatsContext, SourceType};
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use parquet::{
    arrow::{
        arrow_writer::{compute_leaves, get_column_writers, ArrowLeafColumn},
        ArrowSchemaConverter,
    },
    basic::Compression,
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    schema::types::SchemaDescriptor,
};
use tokio::sync::Mutex;

use crate::{FileWriter, WriterFactory};

/// TODO(desmond): This can be tuned.
/// Default buffer size for writing to files.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// PhysicalWriterFactory is a factory for creating physical writers, i.e. parquet, csv writers.
pub struct PhysicalWriterFactory {
    output_file_info: OutputFileInfo,
    schema: SchemaRef,
    native: bool,
}

impl PhysicalWriterFactory {
    fn native_available(file_schema: &SchemaRef) -> bool {
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
                        // TODO(desmond): Support extension and timestamp types.
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

    pub fn new(output_file_info: OutputFileInfo, file_schema: &SchemaRef, native: bool) -> Self {
        Self {
            output_file_info,
            schema: file_schema.clone(),
            native: native && Self::native_available(file_schema),
        }
    }
}

impl WriterFactory for PhysicalWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        let (source_type, root_dir) = parse_url(&self.output_file_info.root_dir)?;
        match self.native {
            // TODO(desmond): Remote writes.
            true if matches!(source_type, SourceType::File) => {
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

                let arrow_schema = Arc::new(self.schema.to_arrow()?.into());

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
            _ => {
                let writer = create_pyarrow_file_writer(
                    &self.output_file_info.root_dir,
                    file_idx,
                    self.output_file_info.compression.as_ref(),
                    self.output_file_info.io_config.as_ref(),
                    self.output_file_info.file_format,
                    partition_values,
                )?;
                Ok(writer)
            }
        }
    }
}

struct ArrowParquetWriter {
    filename: PathBuf,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    partition_values: Option<RecordBatch>,
    file_writer: Option<Arc<Mutex<SerializedFileWriter<BufWriter<std::fs::File>>>>>,
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
        self.file_writer = Some(Arc::new(Mutex::new(writer)));
        // TODO(desmond): Currently using the IO runtime because blocking doesn't seem to work on the compute runtime.
        self.compute_runtime = Some(get_io_runtime(true));
        Ok(())
    }
}

impl FileWriter for ArrowParquetWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        if self.file_writer.is_none() {
            self.create_writer()?;
        }
        let compute_runtime = self
            .compute_runtime
            .as_ref()
            .expect("Compute runtime should be created by now");
        let starting_bytes_written = self.bytes_written();
        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        let record_batches =
            data.tables_or_read(IOStatsContext::new("ArrowParquetWriter::write"))?;
        let mut column_writer_worker_threads: Vec<_> = column_writers
            .into_iter()
            .map(|mut col_writer| {
                let (send, mut recv) =
                    tokio::sync::mpsc::channel::<ArrowLeafColumn>(record_batches.len());
                let handle = compute_runtime.spawn(async move {
                    // receive Arrays to encode via the channel
                    while let Some(col) = recv.recv().await {
                        col_writer.write(&col)?;
                    }
                    // Once the input is complete, close the writer to return the newly created ArrowColumnChunk
                    col_writer.close()
                });
                (handle, send)
            })
            .collect();

        let file_writer = self.file_writer.clone();
        let fields = self.arrow_schema.fields.clone();
        compute_runtime.block_on(async move {
            for record_batch in record_batches.iter() {
                let mut worker_iter = column_writer_worker_threads.iter_mut();
                let arrays = record_batch.get_inner_arrow_arrays();
                for (arr, field) in arrays.zip(&fields) {
                    for leaves in compute_leaves(field, &arr.into())
                        .map_err(|e| DaftError::ParquetError(e.to_string()))?
                    {
                        let _ = worker_iter
                            .next()
                            .expect("Worker iterator should still be valid")
                            .1
                            .send(leaves)
                            .await;
                    }
                }
            }
            // Wait for the workers to complete encoding, and append the resulting column chunks to the row group and the file.
            let handles: Vec<_> = column_writer_worker_threads
                .into_iter()
                .map(|(handle, send)| {
                    // Drop send side to signal termination.
                    drop(send);
                    handle
                })
                .collect();
            let mut file_writer = file_writer
                .as_ref()
                .expect("File writer should be created by now")
                .lock()
                .await;
            let mut row_group_writer = file_writer
                .next_row_group()
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;
            for handle in handles {
                let chunk = handle
                    .await?
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                chunk
                    .append_to_row_group(&mut row_group_writer)
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;
            }
            // Close the row group which writes to the underlying file
            row_group_writer
                .close()
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;
            Ok::<(), DaftError>(())
        })??;

        let bytes_written = self.bytes_written() - starting_bytes_written;
        Ok(bytes_written)
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.
        let file_writer = self.file_writer.clone();
        self.compute_runtime
            .as_ref()
            .expect("IO runtime should be created by now")
            .block_on(async move {
                let mut file_writer = file_writer
                    .as_ref()
                    .expect("File writer should be created by now")
                    .lock()
                    .await;
                let _metadata = file_writer
                    .finish()
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                Ok::<(), DaftError>(())
            })??;

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
        let file_writer = self.file_writer.clone();
        let bytes_written = self
            .compute_runtime
            .as_ref()
            .expect("IO runtime should be created by now")
            .block_on(async move {
                let file_writer = file_writer
                    .as_ref()
                    .expect("File writer should be created by now")
                    .lock()
                    .await;

                file_writer.bytes_written()
            })
            .unwrap_or(0);
        bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        let file_writer = self.file_writer.clone();
        let bytes_written = self
            .compute_runtime
            .as_ref()
            .expect("IO runtime should be created by now")
            .block_on(async move {
                let file_writer = file_writer
                    .as_ref()
                    .expect("File writer should be created by now")
                    .lock()
                    .await;

                file_writer.bytes_written()
            })
            .unwrap_or(0);
        vec![bytes_written]
    }
}

pub fn create_pyarrow_file_writer(
    root_dir: &str,
    file_idx: usize,
    compression: Option<&String>,
    io_config: Option<&daft_io::IOConfig>,
    format: FileFormat,
    partition: Option<&RecordBatch>,
) -> DaftResult<Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>> {
    match format {
        #[cfg(feature = "python")]
        FileFormat::Parquet => Ok(Box::new(crate::pyarrow::PyArrowWriter::new_parquet_writer(
            root_dir,
            file_idx,
            compression,
            io_config,
            partition,
        )?)),
        #[cfg(feature = "python")]
        FileFormat::Csv => Ok(Box::new(crate::pyarrow::PyArrowWriter::new_csv_writer(
            root_dir, file_idx, io_config, partition,
        )?)),
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for physical write".to_string(),
        )),
    }
}
