use std::sync::Arc;

use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::simple::common::{Source, SourceResultType};

pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
    next_scan_task_idx: usize,
}
impl ScanTaskSource {
    pub fn new(scan_tasks: Vec<Arc<ScanTask>>) -> ScanTaskSource {
        ScanTaskSource {
            scan_tasks,
            next_scan_task_idx: 0,
        }
    }
}

impl Source for ScanTaskSource {
    fn get_data(&mut self) -> DaftResult<SourceResultType> {
        println!("ScanTaskSource::get_data");
        if self.next_scan_task_idx >= self.scan_tasks.len() {
            return Ok(SourceResultType::Done);
        }
        let scan_task = self.scan_tasks[self.next_scan_task_idx].clone();
        self.next_scan_task_idx += 1;

        let io_stats = IOStatsContext::new(format!(
            "MicroPartition::from_scan_task for {:?}",
            scan_task.sources
        ));
        let out = MicroPartition::from_scan_task(scan_task, io_stats)?;
        Ok(SourceResultType::HasMoreData(Arc::new(out)))
    }
}
