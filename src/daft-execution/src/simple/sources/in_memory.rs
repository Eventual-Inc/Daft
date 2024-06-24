use std::sync::Arc;

use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::simple::common::{Source, SourceResultType};

pub struct InMemoryScanSource {
    partitions: Vec<Arc<MicroPartition>>,
    next_partition_idx: usize,
}
impl InMemoryScanSource {
    pub fn new(partitions: Vec<Arc<MicroPartition>>) -> InMemoryScanSource {
        InMemoryScanSource {
            partitions,
            next_partition_idx: 0,
        }
    }
}

impl Source for InMemoryScanSource {
    fn get_data(&mut self) -> DaftResult<SourceResultType> {
        println!("ScanTaskSource::get_data");
        if self.next_partition_idx >= self.partitions.len() {
            return Ok(SourceResultType::Done);
        }
        let partition = self.partitions[self.next_partition_idx].clone();
        self.next_partition_idx += 1;

        Ok(SourceResultType::HasMoreData(partition))
    }
}
