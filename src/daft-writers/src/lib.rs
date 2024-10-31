#![feature(hash_raw_entry)]
#![feature(let_chains)]
mod batch;
mod file;
mod partition;
mod physical;

#[cfg(test)]
mod test;

#[cfg(feature = "python")]
mod catalog;
#[cfg(feature = "python")]
mod python;

use std::{cmp::min, sync::Arc};

use batch::TargetBatchWriterFactory;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_plan::OutputFileInfo;
use daft_table::Table;
use file::TargetFileSizeWriterFactory;
use partition::PartitionedWriterFactory;
use physical::PhysicalWriterFactory;

/// This trait is used to abstract the writing of data to a file.
/// The `Input` type is the type of data that will be written to the file.
/// The `Result` type is the type of the result that will be returned when the file is closed.
pub trait FileWriter: Send + Sync {
    type Input;
    type Result;

    /// Write data to the file.
    fn write(&mut self, data: &Self::Input) -> DaftResult<()>;

    /// Close the file and return the result. The caller should NOT write to the file after calling this method.
    fn close(&mut self) -> DaftResult<Self::Result>;
}

/// This trait is used to abstract the creation of a `FileWriter`
/// The `create_writer` method is used to create a new `FileWriter`.
/// `file_idx` is the index of the file that will be written to.
/// `partition_values` is the partition values of the data that will be written to the file.
pub trait WriterFactory: Send + Sync {
    type Input;
    type Result;
    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>>;
}

pub fn make_physical_writer_factory(
    file_info: &OutputFileInfo,
    schema: &SchemaRef,
    cfg: &DaftExecutionConfig,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>> {
    let estimated_row_size_bytes = schema.estimate_row_size_bytes();
    let base_writer_factory = PhysicalWriterFactory::new(file_info.clone());
    match file_info.file_format {
        FileFormat::Parquet => {
            let target_in_memory_file_size =
                cfg.parquet_target_filesize as f64 * cfg.parquet_inflation_factor;
            let target_in_memory_row_group_size =
                cfg.parquet_target_row_group_size as f64 * cfg.parquet_inflation_factor;

            let target_file_rows = if estimated_row_size_bytes > 0.0 {
                target_in_memory_file_size / estimated_row_size_bytes
            } else {
                target_in_memory_file_size
            } as usize;

            let target_row_group_rows = min(
                target_file_rows,
                if estimated_row_size_bytes > 0.0 {
                    target_in_memory_row_group_size / estimated_row_size_bytes
                } else {
                    target_in_memory_row_group_size
                } as usize,
            );

            let row_group_writer_factory =
                TargetBatchWriterFactory::new(Arc::new(base_writer_factory), target_row_group_rows);

            let file_writer_factory = TargetFileSizeWriterFactory::new(
                Arc::new(row_group_writer_factory),
                target_file_rows,
            );

            if let Some(partition_cols) = &file_info.partition_cols {
                let partitioned_writer_factory = PartitionedWriterFactory::new(
                    Arc::new(file_writer_factory),
                    partition_cols.clone(),
                );
                Arc::new(partitioned_writer_factory)
            } else {
                Arc::new(file_writer_factory)
            }
        }
        FileFormat::Csv => {
            let target_in_memory_file_size =
                cfg.csv_target_filesize as f64 * cfg.csv_inflation_factor;
            let target_file_rows = if estimated_row_size_bytes > 0.0 {
                target_in_memory_file_size / estimated_row_size_bytes
            } else {
                target_in_memory_file_size
            } as usize;

            let file_writer_factory =
                TargetFileSizeWriterFactory::new(Arc::new(base_writer_factory), target_file_rows);

            if let Some(partition_cols) = &file_info.partition_cols {
                let partitioned_writer_factory = PartitionedWriterFactory::new(
                    Arc::new(file_writer_factory),
                    partition_cols.clone(),
                );
                Arc::new(partitioned_writer_factory)
            } else {
                Arc::new(file_writer_factory)
            }
        }
        _ => unreachable!("Physical write should only support Parquet and CSV"),
    }
}

#[cfg(feature = "python")]
pub fn make_catalog_writer_factory(
    catalog_info: &daft_plan::CatalogType,
    schema: &SchemaRef,
    cfg: &DaftExecutionConfig,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>> {
    use catalog::CatalogWriterFactory;
    use daft_dsl::col;
    use daft_plan::CatalogType;

    let estimated_row_size_bytes = schema.estimate_row_size_bytes();
    let base_writer_factory = CatalogWriterFactory::new(catalog_info.clone());
    let target_in_memory_file_size =
        cfg.parquet_target_filesize as f64 * cfg.parquet_inflation_factor;
    let target_in_memory_row_group_size =
        cfg.parquet_target_row_group_size as f64 * cfg.parquet_inflation_factor;

    let target_file_rows = if estimated_row_size_bytes > 0.0 {
        target_in_memory_file_size / estimated_row_size_bytes
    } else {
        target_in_memory_file_size
    } as usize;

    let target_row_group_rows = min(
        target_file_rows,
        if estimated_row_size_bytes > 0.0 {
            target_in_memory_row_group_size / estimated_row_size_bytes
        } else {
            target_in_memory_row_group_size
        } as usize,
    );

    let row_group_writer_factory =
        TargetBatchWriterFactory::new(Arc::new(base_writer_factory), target_row_group_rows);

    let file_writer_factory =
        TargetFileSizeWriterFactory::new(Arc::new(row_group_writer_factory), target_file_rows);

    match catalog_info {
        CatalogType::Iceberg(ic) if !ic.partition_cols.is_empty() => {
            let partitioned_writer_factory = PartitionedWriterFactory::new(
                Arc::new(file_writer_factory),
                ic.partition_cols.clone(),
            );
            Arc::new(partitioned_writer_factory)
        }
        CatalogType::DeltaLake(dl) => {
            if let Some(partition_cols) = &dl.partition_cols
                && !partition_cols.is_empty()
            {
                println!("partition_cols: {:?}", partition_cols);
                let partition_col_exprs = partition_cols
                    .iter()
                    .map(|name| col(name.as_str()))
                    .collect::<Vec<_>>();
                let partitioned_writer_factory = PartitionedWriterFactory::new(
                    Arc::new(file_writer_factory),
                    partition_col_exprs,
                );
                Arc::new(partitioned_writer_factory)
            } else {
                Arc::new(file_writer_factory)
            }
        }
        _ => Arc::new(file_writer_factory),
    }
}
