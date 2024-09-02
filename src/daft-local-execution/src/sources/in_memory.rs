use std::sync::Arc;

use crate::ExecutionRuntimeHandle;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::sources::source::SourceStream;

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
        _maintain_order: bool,
        _runtime_handle: &mut ExecutionRuntimeHandle,
        _io_stats: IOStatsRef,
    ) -> crate::Result<SourceStream<'static>> {
        let data = self.data.clone();
        Ok(Box::pin(futures::stream::iter(data)))
    }
    fn name(&self) -> &'static str {
        "InMemory"
    }
}
