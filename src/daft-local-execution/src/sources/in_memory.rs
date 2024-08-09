use std::sync::Arc;

use daft_micropartition::MicroPartition;
use futures::{stream, StreamExt};
use tracing::instrument;

use super::source::{Source, SourceStream};

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
    #[instrument(name = "InMemorySource::get_data", level = "info", skip(self))]
    fn get_data(&self, maintain_order: bool) -> SourceStream {
        stream::iter(self.data.clone().into_iter().map(Ok)).boxed()
    }
    fn name(&self) -> &'static str {
        "InMemory"
    }
}
