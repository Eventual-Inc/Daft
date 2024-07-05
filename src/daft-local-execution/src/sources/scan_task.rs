use std::{pin::Pin, sync::Arc};

use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::{stream, Stream};

use super::source::Source;

#[derive(Clone)]
pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
}

impl ScanTaskSource {
    pub fn new(scan_tasks: Vec<Arc<ScanTask>>) -> Self {
        Self { scan_tasks }
    }
}

impl Source for ScanTaskSource {
    fn get_data(&self) -> Pin<Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
        let stream = stream::iter(self.scan_tasks.clone().into_iter().map(|scan_task| {
            let io_stats = IOStatsContext::new("MicroPartition::from_scan_task");
            let out =
                std::thread::spawn(move || MicroPartition::from_scan_task(scan_task, io_stats))
                    .join()
                    .unwrap()?;
            Ok(Arc::new(out))
        }));
        Box::pin(stream)
    }
}
