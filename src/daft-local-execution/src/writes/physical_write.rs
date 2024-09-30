use common_error::DaftResult;
use daft_micropartition::{create_file_writer, FileWriter};
use daft_plan::OutputFileInfo;
use daft_table::Table;

use super::unpartitioned_write::WriteOperator;

pub(crate) struct PhysicalWriteOperator {
    output_file_info: OutputFileInfo,
}

impl PhysicalWriteOperator {
    pub(crate) fn new(output_file_info: OutputFileInfo) -> Self {
        Self { output_file_info }
    }
}

impl WriteOperator for PhysicalWriteOperator {
    fn name(&self) -> &'static str {
        "PhysicalWriteOperator"
    }
    fn create_writer(&self, file_idx: usize) -> DaftResult<Box<dyn FileWriter>> {
        let writer = create_file_writer(
            &self.output_file_info.root_dir,
            file_idx,
            &self.output_file_info.compression,
            &self.output_file_info.io_config,
            self.output_file_info.file_format,
            None,
        )?;
        Ok(writer)
    }
    fn create_partitioned_writer(
        &self,
        file_idx: usize,
        partition_value: &Table,
        postfix: &str,
    ) -> DaftResult<Box<dyn FileWriter>> {
        let root_dir = format!("{}/{}", self.output_file_info.root_dir, postfix);
        let writer = create_file_writer(
            &root_dir,
            file_idx,
            &self.output_file_info.compression,
            &self.output_file_info.io_config,
            self.output_file_info.file_format,
            Some(partition_value),
        )?;
        Ok(writer)
    }
}
