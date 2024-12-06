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
    cmp::min,
    sync::{Arc, Mutex},
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

    /// Write data to the file, returning the number of bytes written.
    fn write(&mut self, data: Self::Input) -> DaftResult<usize>;

    /// Close the file and return the result. The caller should NOT write to the file after calling this method.
    fn close(&mut self) -> DaftResult<Self::Result>;

    /// Return the total number of bytes written by this writer.
    fn bytes_written(&self) -> usize;
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
            let file_size_calculator = TargetInMemorySizeBytesCalculator::new(
                cfg.parquet_target_filesize,
                cfg.parquet_inflation_factor,
            );
            let row_group_size_calculator = TargetInMemorySizeBytesCalculator::new(
                min(
                    cfg.parquet_target_row_group_size,
                    cfg.parquet_target_filesize,
                ),
                cfg.parquet_inflation_factor,
            );
            let row_group_writer_factory = TargetBatchWriterFactory::new(
                Arc::new(base_writer_factory),
                Arc::new(row_group_size_calculator),
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

    let file_size_calculator = TargetInMemorySizeBytesCalculator::new(
        cfg.parquet_target_filesize,
        cfg.parquet_inflation_factor,
    );
    let row_group_size_calculator = TargetInMemorySizeBytesCalculator::new(
        min(
            cfg.parquet_target_row_group_size,
            cfg.parquet_target_filesize,
        ),
        cfg.parquet_inflation_factor,
    );
    let row_group_writer_factory = TargetBatchWriterFactory::new(
        Arc::new(base_writer_factory),
        Arc::new(row_group_size_calculator),
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

/// This struct is used to adaptively calculate the target in-memory size of a table
/// given a target on-disk size, and the actual on-disk size of the written data.
pub(crate) struct TargetInMemorySizeBytesCalculator {
    target_size_bytes: usize,
    state: Mutex<CalculatorState>,
}

struct CalculatorState {
    estimated_inflation_factor: f64,
    num_samples: usize,
}

impl TargetInMemorySizeBytesCalculator {
    fn new(target_size_bytes: usize, initial_inflation_factor: f64) -> Self {
        assert!(target_size_bytes > 0 && initial_inflation_factor > 0.0);
        Self {
            target_size_bytes,
            state: Mutex::new(CalculatorState {
                estimated_inflation_factor: initial_inflation_factor,
                num_samples: 0,
            }),
        }
    }

    fn calculate_target_in_memory_size_bytes(&self) -> usize {
        let state = self.state.lock().unwrap();
        let factor = state.estimated_inflation_factor;
        (self.target_size_bytes as f64 * factor) as usize
    }

    pub fn record_and_update_inflation_factor(
        &self,
        actual_on_disk_size_bytes: usize,
        estimate_in_memory_size_bytes: usize,
    ) {
        // Avoid division by zero - in practice actual_on_disk_size_bytes should never be 0
        // as we're dealing with real files, but be defensive
        if actual_on_disk_size_bytes == 0 {
            return;
        }
        let new_inflation_factor =
            estimate_in_memory_size_bytes as f64 / actual_on_disk_size_bytes as f64;
        let mut state = self.state.lock().unwrap();

        state.num_samples += 1;

        // Calculate running average:
        // For n samples, new average = (previous_avg * (n-1)/n) + (new_value * 1/n)
        // This ensures each sample's final weight is 1/n in the average.
        // For example:
        // - First sample (n=1): Uses just the new value
        // - Second sample (n=2): Weights previous and new equally (1/2 each)
        // - Third sample (n=3): Weights are 2/3 previous (preserving equal weight of its samples)
        //                       and 1/3 new
        let new_factor = if state.num_samples == 1 {
            new_inflation_factor
        } else {
            state.estimated_inflation_factor.mul_add(
                (state.num_samples - 1) as f64 / state.num_samples as f64,
                new_inflation_factor * (1.0 / state.num_samples as f64),
            )
        };

        state.estimated_inflation_factor = new_factor;
    }
}
