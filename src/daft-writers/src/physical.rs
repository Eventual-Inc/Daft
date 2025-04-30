use std::{io::BufWriter, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
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

use crate::{FileWriter, WriterFactory};

/// Default buffer size for writing to files. We choose 132MiB since row groups are best-effort kept to ~128MiB.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 132 * 1024 * 1024;

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
        let (source_type, _) = parse_url(&self.output_file_info.root_dir)?;
        match self.native {
            // TODO(desmond): Remote writes.
            // TODO(desmond): Proper error handling.
            true if matches!(source_type, SourceType::File) => {
                let dir = if let Some(partition_values) = partition_values {
                    let partition_string = partition_values.to_partition_string(None)?;
                    format!("{}{}", self.output_file_info.root_dir, partition_string)
                } else {
                    self.output_file_info.root_dir.to_string()
                };
                // Create the directories if they don't exist.
                std::fs::create_dir_all(&dir)?;
                let filename = format!("{}/{}-{}.parquet", dir, uuid::Uuid::new_v4(), file_idx);

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
                    .unwrap();

                Ok(Box::new(ArrowParquetWriter {
                    filename,
                    writer_properties,
                    arrow_schema,
                    parquet_schema,
                    file_writer: None,
                    partition_values: partition_values.cloned(),
                }))
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
    filename: String,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    file_writer: Option<SerializedFileWriter<BufWriter<std::fs::File>>>,
    partition_values: Option<RecordBatch>,
}

impl ArrowParquetWriter {
    fn create_writer(&mut self) -> DaftResult<()> {
        let file = std::fs::File::create(&self.filename)?;
        let bufwriter = BufWriter::with_capacity(DEFAULT_WRITE_BUFFER_SIZE, file);
        let writer = SerializedFileWriter::new(
            bufwriter,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::InternalError(e.to_string()))?;
        self.file_writer = Some(writer);
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
        let file_writer = self
            .file_writer
            .as_mut()
            .expect("File writer should be created by now");
        let current_bytes_written = file_writer.bytes_written();
        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .unwrap();
        let mut workers: Vec<_> = column_writers
            .into_iter()
            .map(|mut col_writer| {
                let (send, recv) = std::sync::mpsc::channel::<ArrowLeafColumn>();
                let handle = std::thread::spawn(move || {
                    // receive Arrays to encode via the channel
                    for col in recv {
                        col_writer.write(&col)?;
                    }
                    // once the input is complete, close the writer
                    // to return the newly created ArrowColumnChunk
                    col_writer.close()
                });
                (handle, send)
            })
            .collect();
        let mut row_group_writer = file_writer.next_row_group().unwrap();

        let record_batches =
            data.tables_or_read(IOStatsContext::new("ArrowParquetWriter::write"))?;
        // compute_leaves(field, array)
        for record_batch in record_batches.iter() {
            let mut worker_iter = workers.iter_mut();
            let arrays = record_batch.get_inner_arrow_arrays();
            for (arr, field) in arrays.zip(&self.arrow_schema.fields) {
                for leaves in compute_leaves(field, &arr.into()).unwrap() {
                    worker_iter.next().unwrap().1.send(leaves).unwrap();
                }
            }
        }

        // Wait for the workers to complete encoding, and append
        // the resulting column chunks to the row group (and the file)
        for (handle, send) in workers {
            drop(send); // Drop send side to signal termination
                        // wait for the worker to send the completed chunk
            let chunk = handle.join().unwrap().unwrap();
            chunk.append_to_row_group(&mut row_group_writer).unwrap();
        }
        // Close the row group which writes to the underlying file
        row_group_writer.close().unwrap();

        let bytes_written = file_writer.bytes_written() - current_bytes_written;
        Ok(bytes_written)
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.
        let file_writer = self.file_writer.as_mut().ok_or_else(|| {
            DaftError::InternalError(
                "File writer should be created if close() is called".to_string(),
            )
        })?;

        let _metadata = file_writer.finish().unwrap();
        let field = Field::new("path", DataType::Utf8);
        let filename_series = Series::from_arrow(
            Arc::new(field.clone()),
            Box::new(arrow2::array::Utf8Array::<i64>::from_slice(
                [&self.filename],
            )),
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
            .map(|w| w.bytes_written())
            .unwrap_or(0)
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        self.file_writer
            .as_ref()
            .map(|w| vec![w.bytes_written()])
            .unwrap_or_else(|| vec![0])
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
