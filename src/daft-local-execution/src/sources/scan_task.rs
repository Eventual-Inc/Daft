use std::sync::Arc;

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::{stream, StreamExt, TryStreamExt};

use super::source::{Source, SourceStream};

pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
}

impl ScanTaskSource {
    pub fn new(scan_tasks: Vec<Arc<ScanTask>>) -> Self {
        Self { scan_tasks }
    }
}

impl Source for ScanTaskSource {
    fn get_data(&self) -> SourceStream {
        let scan_tasks = self.scan_tasks.clone();
        std::thread::spawn(move || {
            let streams = scan_tasks.into_iter().map(|scan_task| {
                let io_stats = IOStatsContext::new("MicroPartition::from_scan_task");
                MicroPartition::from_scan_task_streaming(scan_task, io_stats).unwrap()
            });

            let combined_stream = stream::select_all(streams);
            combined_stream.map_ok(Arc::new).boxed()
        })
        .join()
        .expect("Failed to join thread.")
    }
}
