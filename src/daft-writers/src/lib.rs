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
mod lance;
#[cfg(feature = "python")]
mod pyarrow;

use std::{
    cmp::max,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use batch::TargetBatchWriterFactory;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_dsl::ExprRef;
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_table::Table;
use file::TargetFileSizeWriterFactory;
#[cfg(feature = "python")]
pub use lance::make_lance_writer_factory;
use partition::PartitionedWriterFactory;
use physical::PhysicalWriterFactory;
/// This trait is used to abstract the writing of data to a file.
/// The `Input` type is the type of data that will be written to the file.
/// The `Result` type is the type of the result that will be returned when the file is closed.
pub trait FileWriter: Send + Sync {
    type Input;
    type Result;

    /// Write data to the file.
    fn write(&mut self, data: Self::Input) -> DaftResult<()>;

    /// Close the file and return the result. The caller should NOT write to the file after calling this method.
    fn close(&mut self) -> DaftResult<Self::Result>;

    /// Return the current position of the file, if applicable.
    fn tell(&self) -> DaftResult<Option<usize>>;
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
    cfg: &DaftExecutionConfig,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>> {
    let base_writer_factory = PhysicalWriterFactory::new(file_info.clone());
    match file_info.file_format {
        FileFormat::Parquet => {
            let row_group_size_calculator = TargetInMemorySizeBytesCalculator::new(
                cfg.parquet_target_row_group_size,
                cfg.parquet_inflation_factor,
            );
            let row_group_writer_factory = TargetBatchWriterFactory::new(
                Arc::new(base_writer_factory),
                Arc::new(row_group_size_calculator),
            );

            let file_size_calculator = TargetInMemorySizeBytesCalculator::new(
                max(
                    cfg.parquet_target_filesize,
                    cfg.parquet_target_row_group_size,
                ),
                cfg.parquet_inflation_factor,
            );
            let file_writer_factory = TargetFileSizeWriterFactory::new(
                Arc::new(row_group_writer_factory),
                Arc::new(file_size_calculator),
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
            let file_size_calculator = TargetInMemorySizeBytesCalculator::new(
                cfg.csv_target_filesize,
                cfg.csv_inflation_factor,
            );

            let file_writer_factory = TargetFileSizeWriterFactory::new(
                Arc::new(base_writer_factory),
                Arc::new(file_size_calculator),
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
        _ => unreachable!("Physical write should only support Parquet and CSV"),
    }
}

#[cfg(feature = "python")]
pub fn make_catalog_writer_factory(
    catalog_info: &daft_logical_plan::CatalogType,
    partition_cols: &Option<Vec<ExprRef>>,
    cfg: &DaftExecutionConfig,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>> {
    use catalog::CatalogWriterFactory;

    let base_writer_factory = CatalogWriterFactory::new(catalog_info.clone());

    let row_group_size_calculator = TargetInMemorySizeBytesCalculator::new(
        cfg.parquet_target_row_group_size,
        cfg.parquet_inflation_factor,
    );
    let row_group_writer_factory = TargetBatchWriterFactory::new(
        Arc::new(base_writer_factory),
        Arc::new(row_group_size_calculator),
    );

    let file_size_calculator = TargetInMemorySizeBytesCalculator::new(
        max(
            cfg.parquet_target_filesize,
            cfg.parquet_target_row_group_size,
        ),
        cfg.parquet_inflation_factor,
    );
    let file_writer_factory = TargetFileSizeWriterFactory::new(
        Arc::new(row_group_writer_factory),
        Arc::new(file_size_calculator),
    );

    if let Some(partition_cols) = partition_cols {
        let partitioned_writer_factory =
            PartitionedWriterFactory::new(Arc::new(file_writer_factory), partition_cols.clone());
        Arc::new(partitioned_writer_factory)
    } else {
        Arc::new(file_writer_factory)
    }
}

pub struct TargetInMemorySizeBytesCalculator {
    estimated_inflation_factor: AtomicU64, // Using u64 to store f64 bits
    target_size_bytes: usize,
    num_samples: AtomicUsize,
}

impl TargetInMemorySizeBytesCalculator {
    fn new(target_size_bytes: usize, initial_inflation_factor: f64) -> Self {
        Self {
            estimated_inflation_factor: AtomicU64::new(initial_inflation_factor.to_bits()),
            target_size_bytes,
            num_samples: AtomicUsize::new(0),
        }
    }

    fn calculate_target_in_memory_size_bytes(&self) -> usize {
        let factor = f64::from_bits(self.estimated_inflation_factor.load(Ordering::Relaxed));
        (self.target_size_bytes as f64 * factor) as usize
    }

    fn record_and_update_inflation_factor(&self, actual_size_bytes: usize) -> f64 {
        let new_inflation_factor = actual_size_bytes as f64 / self.target_size_bytes as f64;
        let new_num_samples = self.num_samples.fetch_add(1, Ordering::Relaxed) + 1;

        let current_factor =
            f64::from_bits(self.estimated_inflation_factor.load(Ordering::Relaxed));

        let new_factor = if new_num_samples == 1 {
            new_inflation_factor
        } else {
            current_factor.mul_add(
                (new_num_samples - 1) as f64 / new_num_samples as f64,
                new_inflation_factor * (1.0 / new_num_samples as f64),
            )
        };

        self.estimated_inflation_factor
            .store(new_factor.to_bits(), Ordering::Relaxed);
        new_factor
    }
}
