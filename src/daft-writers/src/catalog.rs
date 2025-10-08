use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::{CatalogType, DeltaLakeCatalogInfo, IcebergCatalogInfo};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, WriterFactory, pyarrow::PyArrowWriter};

/// CatalogWriterFactory is a factory for creating Catalog writers, i.e. iceberg, delta writers.
pub struct CatalogWriterFactory {
    catalog_info: CatalogType<BoundExpr>,
    native: bool, // TODO: Implement native writer
}

impl CatalogWriterFactory {
    pub fn new(catalog_info: CatalogType<BoundExpr>) -> Self {
        Self {
            catalog_info,
            native: false,
        }
    }
}

impl WriterFactory for CatalogWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
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
    partition_values: Option<&RecordBatch>,
    catalog_info: &CatalogType<BoundExpr>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
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
                io_config.as_ref(),
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
                io_config.as_ref(),
            )?;
            Ok(Box::new(writer))
        }
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for physical write".to_string(),
        )),
    }
}
