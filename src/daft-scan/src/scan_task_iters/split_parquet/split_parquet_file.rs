use common_error::DaftResult;

use crate::ScanTaskRef;

/// Splits its internal ScanTask into smaller ScanTasks based on certain criteria, including
/// the size of the Parquet file and available rowgroups.
///
/// # Implementation Details
///
/// This type implements [`Iterator`] to produce [`ScanTaskRef`]s representing the split tasks.
pub(super) struct ParquetFileSplitter {}

impl Iterator for ParquetFileSplitter {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!("Split the parquet file");
    }
}
