use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use futures::StreamExt;
use std::sync::Arc;

use crate::{
    channel::{create_channel, SingleSender},
    DEFAULT_MORSEL_SIZE,
};

use super::source::{Source, SourceStream};

use tracing::instrument;

#[derive(Debug)]
pub struct ScanTaskSource {
    scan_tasks: Vec<Arc<ScanTask>>,
}

impl ScanTaskSource {
    pub fn new(scan_tasks: Vec<Arc<ScanTask>>) -> Self {
        Self { scan_tasks }
    }

    #[instrument(
        name = "ScanTaskSource::process_scan_task_stream",
        level = "info",
        skip_all
    )]
    async fn process_scan_task_stream(
        scan_task: Arc<ScanTask>,
        sender: SingleSender,
        morsel_size: usize,
    ) {
        let io_stats = IOStatsContext::new("MicroPartition::from_scan_task");
        let stream_result =
            MicroPartition::from_scan_task_streaming(scan_task, io_stats, morsel_size).await;
        match stream_result {
            Ok(mut stream) => {
                while let Some(partition) = stream.next().await {
                    let _ = sender.send(partition).await;
                }
            }
            Err(e) => {
                let _ = sender.send(Err(e.into())).await;
            }
        }
    }
}

impl Source for ScanTaskSource {
    #[instrument(name = "ScanTaskSource::get_data", level = "info", skip(self))]
    fn get_data(&self) -> SourceStream {
        let morsel_size = DEFAULT_MORSEL_SIZE;
        let (mut sender, mut receiver) = create_channel(self.scan_tasks.len(), true);
        for scan_task in self.scan_tasks.clone() {
            tokio::task::spawn(Self::process_scan_task_stream(
                scan_task,
                sender.get_next_sender(),
                morsel_size,
            ));
        }
        Box::pin(async_stream::stream! {
            while let Some(partition) = receiver.recv().await {
                yield partition;
            }
        })
    }
}
