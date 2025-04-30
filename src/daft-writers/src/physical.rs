use std::{
    io::BufWriter,
    sync::{Arc, Mutex},
};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_core::{prelude::*, series::Series};
use daft_io::{parse_url, IOStatsContext, SourceType};
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};

use crate::{FileWriter, WriterFactory};

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
                // TODO(desmond): Explore configurations such data page size limit, writer version, etc. Parquet format v2
                // could be interesting but has much less support in the ecosystem (including ourselves).
                let writer_properties = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_1_0)
                    .set_compression(Compression::SNAPPY)
                    .build();
                let dir = if let Some(partition_values) = partition_values {
                    let partition_string = partition_values.to_partition_string(None)?;
                    format!("{}{}", self.output_file_info.root_dir, partition_string)
                } else {
                    self.output_file_info.root_dir.to_string()
                };
                // Create the directories if they don't exist.
                std::fs::create_dir_all(&dir)?;
                let filename = format!("{}/{}-{}.parquet", dir, uuid::Uuid::new_v4(), file_idx);
                let file = std::fs::File::create(&filename)?;
                let bufwriter = BufWriter::new(file);
                let arrow_rs_schema = Arc::new(self.schema.to_arrow()?.into());
                let writer =
                    ArrowWriter::try_new(bufwriter, arrow_rs_schema, Some(writer_properties))
                        .expect("Failed to create ArrowWriter");
                Ok(Box::new(ArrowParquetWriter {
                    filename,
                    file_writer: Mutex::new(writer),
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
    file_writer: Mutex<ArrowWriter<BufWriter<std::fs::File>>>,
    partition_values: Option<RecordBatch>,
}

impl FileWriter for ArrowParquetWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        let mut file_writer = self.file_writer.lock().expect("Failed to lock file writer");
        let current_bytes_written = file_writer.bytes_written();
        let record_batches =
            data.tables_or_read(IOStatsContext::new("ArrowParquetWriter::write"))?;
        for record_batch in record_batches.iter().cloned() {
            let _ = file_writer.write(&record_batch.try_into()?);
        }
        // Flush the current row group.
        file_writer.flush().unwrap();
        let bytes_written = file_writer.bytes_written() - current_bytes_written;
        Ok(bytes_written)
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.
        let mut file_writer = self.file_writer.lock().unwrap();
        let _metadata = file_writer.finish().unwrap();
        let field = Field::new("path", DataType::Utf8);
        let filename_series = Series::from_arrow(
            Arc::new(field.clone()),
            Box::new(arrow2::array::Utf8Array::<i64>::from_slice(
                [&self.filename],
            )),
        )?;
        let mut record_batch =
            RecordBatch::new_with_size(Schema::new(vec![field]), vec![filename_series], 1)?;
        if let Some(partition_values) = self.partition_values.take() {
            record_batch = record_batch.union(&partition_values)?;
        }
        Ok(Some(record_batch))
    }

    fn bytes_written(&self) -> usize {
        let file_writer = self.file_writer.lock().unwrap();
        file_writer.bytes_written()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        let file_writer = self.file_writer.lock().unwrap();
        vec![file_writer.bytes_written()]
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
