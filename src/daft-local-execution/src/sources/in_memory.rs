use std::sync::Arc;

use crate::{channel::MultiSender, runtime_stats::RuntimeStatsContext, ExecutionRuntimeHandle};
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;

pub struct InMemorySource {
    data: Vec<Arc<MicroPartition>>,
}

impl InMemorySource {
    pub fn new(data: Vec<Arc<MicroPartition>>) -> Self {
        Self { data }
    }
    pub fn boxed(self) -> Box<dyn Source> {
        Box::new(self) as Box<dyn Source>
    }
}

impl Source for InMemorySource {
    #[instrument(name = "InMemorySource::get_data", level = "info", skip_all)]
    fn get_data(
        &self,
        mut destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
        runtime_stats: Arc<RuntimeStatsContext>,
        _io_stats: IOStatsRef,
    ) -> crate::Result<()> {
        let data = self.data.clone();
        runtime_handle.spawn(
            async move {
                for part in data {
                    let len = part.len();
                    let _ = destination.get_next_sender().send(part).await;
                    runtime_stats.mark_rows_emitted(len as u64);
                }
                Ok(())
            },
            self.name(),
        );
        Ok(())
    }
    fn name(&self) -> &'static str {
        "InMemory"
    }
}
