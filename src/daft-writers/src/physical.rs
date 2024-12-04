use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

/// PhysicalWriterFactory is a factory for creating physical writers, i.e. parquet, csv writers.
pub struct PhysicalWriterFactory {
    output_file_info: OutputFileInfo,
    native: bool, // TODO: Implement native writer
}

impl PhysicalWriterFactory {
    pub fn new(output_file_info: OutputFileInfo) -> Self {
        Self {
            output_file_info,
            native: false,
        }
    }
}

impl WriterFactory for PhysicalWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        match self.native {
            true => unimplemented!(),
            false => {
                let writer = create_pyarrow_file_writer(
                    &self.output_file_info.root_dir,
                    file_idx,
                    &self.output_file_info.compression,
                    &self.output_file_info.io_config,
                    self.output_file_info.file_format,
                    partition_values,
                )?;
                Ok(writer)
            }
        }
    }
}

pub fn create_pyarrow_file_writer(
    root_dir: &str,
    file_idx: usize,
    compression: &Option<String>,
    io_config: &Option<daft_io::IOConfig>,
    format: FileFormat,
    partition: Option<&Table>,
) -> DaftResult<Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>> {
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
