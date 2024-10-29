mod batch;
mod physical;
#[cfg(feature = "python")]
mod python;

pub use batch::{TargetBatchWriter, TargetBatchWriterFactory};
use common_error::DaftResult;
use daft_table::Table;
pub use physical::PhysicalWriterFactory;

/// This trait is used to abstract the writing of data to a file.
/// The `Input` type is the type of data that will be written to the file.
/// The `Result` type is the type of the result that will be returned when the file is closed.
pub trait FileWriter: Send + Sync {
    type Input;
    type Result;

    fn write(&mut self, data: &Self::Input) -> DaftResult<()>;
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
