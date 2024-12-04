use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
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
    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for InMemorySource {
    #[instrument(name = "InMemorySource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        Ok(Box::pin(futures::stream::iter(
            self.data.clone().into_iter().map(Ok),
        )))
    }
    fn name(&self) -> &'static str {
        "InMemory"
    }
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
