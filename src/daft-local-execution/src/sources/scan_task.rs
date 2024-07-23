use std::sync::Arc;

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::{stream, StreamExt};
use snafu::ResultExt;

use crate::JoinSnafu;

use super::source::{Source, SourceStream};

use tracing::{instrument, Instrument, Span};

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
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip(self))]
    fn get_data(&self) -> SourceStream {
        let stream = stream::iter(self.scan_tasks.clone().into_iter().map(|scan_task| async {
            tokio::task::spawn_blocking(move || {
                let child_span = tracing::info_span!("ScanTaskSource::from_scan_task");
                let _span = child_span.follows_from(Span::current());
                let _eg = _span.enter();
                let io_stats = IOStatsContext::new("MicroPartition::from_scan_task");
                MicroPartition::from_scan_task(scan_task, io_stats)
                    .map(Arc::new)
                    .map_err(Into::into)
            })
            .in_current_span()
            .await
            .context(JoinSnafu {})?
        }));
        stream.buffered(self.scan_tasks.len()).boxed()
    }
}
