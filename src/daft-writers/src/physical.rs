use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_core::prelude::*;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    json_writer::{create_native_json_writer, native_json_writer_supported},
    parquet_writer::{create_native_parquet_writer, native_parquet_writer_supported},
    AsyncFileWriter, WriterFactory,
};

enum WriterType {
    Native,
    Pyarrow,
}

/// PhysicalWriterFactory is a factory for creating physical writers, i.e. parquet, csv writers.
pub struct PhysicalWriterFactory {
    output_file_info: OutputFileInfo<BoundExpr>,
    schema: SchemaRef,
    writer_type: WriterType,
}

impl PhysicalWriterFactory {
    pub fn new(
        output_file_info: OutputFileInfo<BoundExpr>,
        file_schema: SchemaRef,
        native_enabled: bool,
    ) -> DaftResult<Self> {
        let writer_type =
            Self::select_writer_type(&output_file_info, &file_schema, native_enabled)?;

        Ok(Self {
            output_file_info,
            schema: file_schema,
            writer_type,
        })
    }

    /// Determines which writer type to use based on file format and configuration.
    fn select_writer_type(
        output_file_info: &OutputFileInfo<BoundExpr>,
        file_schema: &SchemaRef,
        native_enabled: bool,
    ) -> DaftResult<WriterType> {
        match output_file_info.file_format {
            FileFormat::Parquet => {
                Self::select_parquet_writer_type(output_file_info, file_schema, native_enabled)
            }
            FileFormat::Json => Self::select_json_writer_type(file_schema),
            _ => Ok(WriterType::Pyarrow), // Default to PyArrow for unsupported formats.
        }
    }

    /// Selects writer type for Parquet format.
    fn select_parquet_writer_type(
        output_file_info: &OutputFileInfo<BoundExpr>,
        file_schema: &SchemaRef,
        native_enabled: bool,
    ) -> DaftResult<WriterType> {
        if !native_enabled {
            return Ok(WriterType::Pyarrow);
        }

        let native_supported =
            native_parquet_writer_supported(&output_file_info.root_dir, file_schema)?;

        if native_supported {
            Ok(WriterType::Native)
        } else {
            Ok(WriterType::Pyarrow)
        }
    }

    fn select_json_writer_type(file_schema: &SchemaRef) -> DaftResult<WriterType> {
        let native_supported = native_json_writer_supported(file_schema)?;
        if !native_supported {
            return Err(DaftError::NotImplemented("JSON writes are not supported with extension, timezone with timestamp, binary, or duration data types".to_string()));
        }
        // There is only a native implementation of the JSON writer. PyArrow does not support JSON writes.
        Ok(WriterType::Native)
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
        match self.writer_type {
            WriterType::Native => create_native_writer(
                &self.output_file_info.root_dir,
                file_idx,
                &self.schema,
                self.output_file_info.file_format,
                partition_values,
                self.output_file_info.io_config.clone(),
            ),
            WriterType::Pyarrow => create_pyarrow_file_writer(
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
    io_config: Option<daft_io::IOConfig>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    match file_format {
        FileFormat::Parquet => {
            create_native_parquet_writer(root_dir, schema, file_idx, partition_values, io_config)
        }
        FileFormat::Json => {
            create_native_json_writer(root_dir, file_idx, partition_values, io_config)
        }
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for native write".to_string(),
        )),
    }
}
