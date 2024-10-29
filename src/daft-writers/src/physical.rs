use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_plan::OutputFileInfo;
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
                #[cfg(feature = "python")]
                {
                    let writer = crate::python::create_pyarrow_file_writer(
                        &self.output_file_info.root_dir,
                        file_idx,
                        &self.output_file_info.compression,
                        &self.output_file_info.io_config,
                        self.output_file_info.file_format,
                        partition_values,
                    )?;
                    Ok(writer)
                }

                #[cfg(not(feature = "python"))]
                {
                    unimplemented!()
                }
            }
        }
    }
}
