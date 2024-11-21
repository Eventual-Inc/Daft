use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_logical_plan::{CatalogType, DeltaLakeCatalogInfo, IcebergCatalogInfo};
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{pyarrow::PyArrowWriter, FileWriter, WriterFactory};

/// CatalogWriterFactory is a factory for creating Catalog writers, i.e. iceberg, delta writers.
pub struct CatalogWriterFactory {
    catalog_info: CatalogType,
    native: bool, // TODO: Implement native writer
}

impl CatalogWriterFactory {
    pub fn new(catalog_info: CatalogType) -> Self {
        Self {
            catalog_info,
            native: false,
        }
    }
}

impl WriterFactory for CatalogWriterFactory {
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
                let writer =
                    create_pyarrow_catalog_writer(file_idx, partition_values, &self.catalog_info)?;
                Ok(writer)
            }
        }
    }
}

pub fn create_pyarrow_catalog_writer(
    file_idx: usize,
    partition_values: Option<&Table>,
    catalog_info: &CatalogType,
) -> DaftResult<Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>> {
    match catalog_info {
        CatalogType::DeltaLake(DeltaLakeCatalogInfo {
            path,
            version,
            large_dtypes,
            io_config,
            ..
        }) => {
            let writer = PyArrowWriter::new_deltalake_writer(
                path,
                file_idx,
                *version,
                *large_dtypes,
                partition_values,
                io_config,
            )?;
            Ok(Box::new(writer))
        }
        CatalogType::Iceberg(IcebergCatalogInfo {
            table_location,
            iceberg_schema,
            iceberg_properties,
            partition_spec_id,
            io_config,
            ..
        }) => {
            let writer = PyArrowWriter::new_iceberg_writer(
                table_location,
                file_idx,
                iceberg_schema,
                iceberg_properties,
                *partition_spec_id,
                partition_values,
                io_config,
            )?;
            Ok(Box::new(writer))
        }
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for physical write".to_string(),
        )),
    }
}
