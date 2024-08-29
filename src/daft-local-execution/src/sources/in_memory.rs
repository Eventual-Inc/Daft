use std::sync::Arc;

use crate::ExecutionRuntimeHandle;
use daft_core::schema::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::sources::source::SourceStream;

pub struct InMemorySource {
    data: Vec<Arc<MicroPartition>>,
    schema: SchemaRef,
}

impl InMemorySource {
    pub fn new(data: Vec<Arc<MicroPartition>>, schema: SchemaRef) -> Self {
        Self { data, schema }
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
        if self.data.is_empty() {
            let empty_mp = Arc::new(MicroPartition::empty(Some(self.schema.clone())));
            return Ok(Box::pin(futures::stream::iter(vec![empty_mp])));
        }
        let data = self.data.clone();
        Ok(Box::pin(futures::stream::iter(data)))
    }
    fn name(&self) -> &'static str {
        "InMemory"
    }
}
