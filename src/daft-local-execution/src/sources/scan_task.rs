use std::sync::Arc;

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::{stream, StreamExt};
use snafu::ResultExt;

use crate::JoinSnafu;

use super::source::{Source, SourceStream};

use tracing::{instrument, Instrument};

#[derive(Debug)]
pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
}

impl ScanTaskSource {
    pub fn new(scan_tasks: Vec<Arc<ScanTask>>) -> Self {
        Self { scan_tasks }
    }
}

impl Source for ScanTaskSource {
    // #[instrument]
    fn get_data(&self) -> SourceStream {
        
        let _span = tracing::info_span!("ScanTaskSource::get_data").entered();

        log::debug!("ScanTaskSource::get_data");
        let stream = stream::iter(self.scan_tasks.clone().into_iter().map(|scan_task| async {
            let child_span = tracing::info_span!("ScanTaskSource::Stream::next");
            tokio::task::spawn_blocking(move || {
                let _span = tracing::info_span!("ScanTaskSource::from_scan_task").entered();
                let io_stats = IOStatsContext::new("MicroPartition::from_scan_task");
                // span.in_scope(|| {
                    MicroPartition::from_scan_task(scan_task, io_stats)
                    .map(Arc::new)
                    .map_err(Into::into)
                // })
            })
            .instrument(child_span)
            .await
            .context(JoinSnafu {})?
            // TODO: Implement dynamic splitting / merging of MicroPartition from scan task
        }));
        stream.buffered(self.scan_tasks.len()).boxed()
    }

    fn name(&self) -> &'static str {
        "ScanTaskSource"
    }
}
