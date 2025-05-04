use std::{
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
    future::Future,
    pin::Pin,
};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_runtime::{get_compute_runtime, get_io_runtime, Runtime, RuntimeTask};
use daft_core::{prelude::*, series::Series};
use daft_io::{parse_url, IOStatsContext, SourceType};
use daft_logical_plan::OutputFileInfo;
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
use tokio::sync::Mutex;

use crate::{FileWriter, WriterFactory};

/// Default buffer size for writing to files. We choose 128MiB since row groups are best-effort kept to ~128MiB.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 128 * 1024 * 1024;

/// PhysicalWriterFactory is a factory for creating physical writers, i.e. parquet, csv writers.
pub struct PhysicalWriterFactory {
    output_file_info: OutputFileInfo,
    schema: SchemaRef,
    native: bool,
}

impl PhysicalWriterFactory {
    pub fn new(output_file_info: OutputFileInfo, file_schema: &SchemaRef, native: bool) -> Self {
        Self {
            output_file_info,
            schema: file_schema.clone(),
            native,
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
                // could be interesting but has much less support in the ecosystem (including by ourselves).
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
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

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
    file_writer: Option<Arc<Mutex<SerializedFileWriter<BufWriter<std::fs::File>>>>>,
    io_runtime: Option<Arc<Runtime>>,
    flusher_task: Option<RuntimeTask<DaftResult<()>>>,
    partition_values: Option<RecordBatch>,
    row_group_sender: Option<tokio::sync::mpsc::UnboundedSender<Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = DaftResult<()>> + Send>> + Send + 'static>>>,
}

impl ArrowParquetWriter {
    fn new(filename: PathBuf, writer_properties: Arc<WriterProperties>, arrow_schema: Arc<arrow_schema::Schema>, parquet_schema: SchemaDescriptor, partition_values: Option<RecordBatch>) -> Self {
        Self {
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            file_writer: None,
            io_runtime: None,
            flusher_task: None,
            partition_values,
            row_group_sender: None,
        }
    }

    fn create_writer(&mut self) -> DaftResult<()> {
        let file = std::fs::File::create(&self.filename)?;
        let bufwriter = BufWriter::new(file);
        let writer = SerializedFileWriter::new(
            bufwriter,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        self.file_writer = Some(Arc::new(Mutex::new(writer)));
        let io_runtime = get_io_runtime(true);
        let (row_group_sender, mut row_group_receiver) = tokio::sync::mpsc::unbounded_channel::<Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = DaftResult<()>> + Send>> + Send + 'static>>();
        let flusher_task = io_runtime.spawn(async move {
            while let Some(row_group_writer) = row_group_receiver.recv().await {
                println!("Received row group writer");
                row_group_writer().await?;
                println!("Flushed row group writer");
            }
            Ok::<(), DaftError>(())
        });
        self.io_runtime = Some(io_runtime);
        self.flusher_task = Some(flusher_task);
        self.row_group_sender = Some(row_group_sender);
        
        Ok(())
    }
}

impl FileWriter for ArrowParquetWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        println!("Write row group");
        if self.file_writer.is_none() {
            self.create_writer()?;
        }
        // let file_writer = self
        //     .file_writer
        //     .expect("File writer should be created by now")
        //     .lock()
        // let current_bytes_written = file_writer.bytes_written();

        let size_estimate = data.size_bytes()?.unwrap_or(0);

        // Get column writers for each leaf column. We'll encode and compress these in parallel
        // and then flush them to the row group writer when they're done.
        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;


        // Process all record batches in the micropartition.
        // TODO(desmond): We should be able to pipeline each record batch with upstream nodes rather than processes all
        // record batches at once.
        let fields = self.arrow_schema.fields.clone();
        let record_batches =
            data.tables_or_read(IOStatsContext::new("ArrowParquetWriter::write"))?;

        let mut column_writer_worker_threads: Vec<_> = column_writers
            .into_iter()
            .map(|mut col_writer| {
                let (send, mut recv) = tokio::sync::mpsc::unbounded_channel::<ArrowLeafColumn>();
                let handle = self.io_runtime.as_ref().expect("IO runtime should be created by now").spawn( async move {
                    // Receive Arrays to encode via the channel.
                    while let Some(col) = recv.recv().await {
                        col_writer.write(&col)?;
                    }
                    // Once the input is complete, close the writer to return the newly created ArrowColumnChunk.
                    col_writer.close()
                });
                (handle, send)
            })
            .collect(); 
        
        for record_batch in record_batches.iter() {
            let mut worker_iter = column_writer_worker_threads.iter_mut();
            let arrays = record_batch.get_inner_arrow_arrays();
            for (arr, field) in arrays.zip(&fields) {
                for leaf in compute_leaves(field, &arr.into())
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?
                {
                    worker_iter.next().unwrap().1.send(leaf);
                }
            }
        }
        let handles: Vec<_> = column_writer_worker_threads.into_iter().map(|(handle, send)| {
            // Drop send side to signal termination.
            drop(send);
            handle
        }).collect();

        let file_writer = self.file_writer.clone();
        let row_group_writer = || Box::pin(async move {
            let mut file_writer = file_writer
                .as_ref()
                .expect("File writer should be created by now")
                .lock()
                .await;
            let mut row_group_writer = file_writer
                .next_row_group()
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;
            // Append the column chunks to the row group and the file.
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
        }) as Pin<Box<dyn Future<Output = DaftResult<()>> + Send>>;
        println!("Sending row group");
        self.row_group_sender.as_ref().expect("Row group sender should be created by now").send(Box::new(row_group_writer));

        // TODO(desmond): maybe we can do better.
        Ok(size_estimate)
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.
        let file_writer = self.file_writer.clone();
        let row_group_sender = self.row_group_sender.take();
        drop(row_group_sender);
        let flusher_task = self.flusher_task.take();
        self.io_runtime.as_ref().expect("IO runtime should be created by now").block_on(async move {
            flusher_task.unwrap().await?;
            let mut file_writer = file_writer
                .as_ref()
                .expect("File writer should be created by now")
                .lock()
                .await;

            let _metadata = file_writer
                .finish()
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;
            Ok::<(), DaftError>(())
        })?;
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
        let bytes_written = self.io_runtime.as_ref().expect("IO runtime should be created by now").block_on(async move {
            let file_writer = file_writer
                .as_ref()
                .expect("File writer should be created by now")
                .lock()
                .await;

            file_writer.bytes_written()
        }).unwrap_or(0);
        bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        let file_writer = self.file_writer.clone();
        let bytes_written = self.io_runtime.as_ref().expect("IO runtime should be created by now").block_on(async move {
            let file_writer = file_writer
                .as_ref()
                .expect("File writer should be created by now")
                .lock()
                .await;

            file_writer.bytes_written()
        }).unwrap_or(0);
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
