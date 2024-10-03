use common_error::DaftResult;
use daft_micropartition::{create_file_writer, FileWriter};
use daft_plan::OutputFileInfo;
use daft_table::Table;

use super::WriterFactory;

pub(crate) struct PhysicalWriterFactory {
    output_file_info: OutputFileInfo,
}

impl PhysicalWriterFactory {
    pub(crate) fn new(output_file_info: OutputFileInfo) -> Self {
        Self { output_file_info }
    }
}

impl WriterFactory for PhysicalWriterFactory {
    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter>> {
        let writer = create_file_writer(
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
