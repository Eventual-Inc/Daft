use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_core::prelude::*;
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    native_arrow_writer::{create_native_parquet_writer, native_writer_supported},
    AsyncFileWriter, WriterFactory,
};

/// PhysicalWriterFactory is a factory for creating physical writers, i.e. parquet, csv writers.
pub struct PhysicalWriterFactory {
    output_file_info: OutputFileInfo,
    schema: SchemaRef,
    native: bool,
}

impl PhysicalWriterFactory {
    pub fn new(
        output_file_info: OutputFileInfo,
        file_schema: SchemaRef,
        native_enabled: bool,
    ) -> DaftResult<Self> {
        let native = native_enabled
            && native_writer_supported(
                output_file_info.file_format,
                &output_file_info.root_dir,
                &file_schema,
            )?;
        Ok(Self {
            output_file_info,
            schema: file_schema,
            native,
        })
    }
}

impl WriterFactory for PhysicalWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        match self.native {
            true => create_native_writer(
                &self.output_file_info.root_dir,
                file_idx,
                &self.schema,
                self.output_file_info.file_format,
                partition_values,
            ),
            _ => create_pyarrow_file_writer(
                &self.output_file_info.root_dir,
                file_idx,
                self.output_file_info.compression.as_ref(),
                self.output_file_info.io_config.as_ref(),
                self.output_file_info.file_format,
                partition_values,
            ),
        }
    }
}

pub fn create_pyarrow_file_writer(
    root_dir: &str,
    file_idx: usize,
    compression: Option<&String>,
    io_config: Option<&daft_io::IOConfig>,
    format: FileFormat,
    partition: Option<&RecordBatch>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
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

fn create_native_writer(
    root_dir: &str,
    file_idx: usize,
    schema: &SchemaRef,
    file_format: FileFormat,
    partition_values: Option<&RecordBatch>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    match file_format {
        FileFormat::Parquet => {
            create_native_parquet_writer(root_dir, schema, file_idx, partition_values)
        }
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for native write".to_string(),
        )),
    }
}
