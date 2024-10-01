use common_error::DaftResult;
use daft_micropartition::{create_deltalake_file_writer, FileWriter};
use daft_plan::DeltaLakeCatalogInfo;
use daft_table::Table;

use super::WriteOperator;

pub(crate) struct DeltalakeWriteOperator {
    deltalake_info: DeltaLakeCatalogInfo,
}

impl DeltalakeWriteOperator {
    pub(crate) fn new(deltalake_info: DeltaLakeCatalogInfo) -> Self {
        Self { deltalake_info }
    }
}

impl WriteOperator for DeltalakeWriteOperator {
    fn name(&self) -> &'static str {
        "DeltalakeWriteOperator"
    }
    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter>> {
        let writer = create_deltalake_file_writer(
            &self.deltalake_info.path,
            file_idx,
            self.deltalake_info.version,
            self.deltalake_info.large_dtypes,
            &self.deltalake_info.io_config,
            partition_values,
        )?;
        Ok(writer)
    }
}
