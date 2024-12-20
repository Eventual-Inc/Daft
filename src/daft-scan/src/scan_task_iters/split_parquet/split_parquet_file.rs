use common_error::DaftResult;

use crate::ScanTaskRef;

pub(super) struct ParquetFileSplitter {}

impl Iterator for ParquetFileSplitter {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!("Split the parquet file");
    }
}
