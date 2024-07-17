use std::sync::Arc;

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::{stream, StreamExt};
use snafu::ResultExt;

use crate::JoinSnafu;

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
        log::debug!("ScanTaskSource::get_data");
        let stream = stream::iter(self.scan_tasks.clone().into_iter().map(|scan_task| async {
            tokio::task::spawn_blocking(move || {
                let io_stats = IOStatsContext::new("MicroPartition::from_scan_task");
                MicroPartition::from_scan_task(scan_task, io_stats)
                    .map(Arc::new)
                    .map_err(Into::into)
            })
            .await
            .context(JoinSnafu {})?
            // TODO: Implement dynamic splitting / merging of MicroPartition from scan task
        }));
        stream.buffered(self.scan_tasks.len()).boxed()
    }
}
