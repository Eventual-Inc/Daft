use common_error::DaftResult;
use daft_micropartition::{create_iceberg_file_writer, FileWriter};
use daft_plan::IcebergCatalogInfo;
use daft_table::Table;

use super::unpartitioned_write::WriteOperator;

pub(crate) struct IcebergWriteOperator {
    iceberg_info: IcebergCatalogInfo,
}

impl IcebergWriteOperator {
    pub(crate) fn new(iceberg_info: IcebergCatalogInfo) -> Self {
        Self { iceberg_info }
    }
}

impl WriteOperator for IcebergWriteOperator {
    fn name(&self) -> &'static str {
        "IcebergWriteOperator"
    }
    fn create_writer(&self, file_idx: usize) -> DaftResult<Box<dyn FileWriter>> {
        let writer = create_iceberg_file_writer(
            &self.iceberg_info.table_location,
            file_idx,
            &None,
            &self.iceberg_info.io_config,
            &self.iceberg_info.iceberg_schema,
            &self.iceberg_info.iceberg_properties,
            &self.iceberg_info.partition_spec,
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
        let root_dir = format!("{}/{}", self.iceberg_info.table_location, postfix);
        let writer = create_iceberg_file_writer(
            &root_dir,
            file_idx,
            &None,
            &self.iceberg_info.io_config,
            &self.iceberg_info.iceberg_schema,
            &self.iceberg_info.iceberg_properties,
            &self.iceberg_info.partition_spec,
            Some(partition_value),
        )?;
        Ok(writer)
    }
}
