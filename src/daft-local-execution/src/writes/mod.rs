use common_error::DaftResult;
use daft_micropartition::FileWriter;
use daft_table::Table;

pub mod partitioned_write;
pub mod physical_write;
pub mod unpartitioned_write;

#[cfg(feature = "python")]
pub mod deltalake_write;
#[cfg(feature = "python")]
pub mod iceberg_write;

pub trait WriteOperator: Send + Sync {
    fn name(&self) -> &'static str;
    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter>>;
}
