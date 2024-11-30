use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::sources::source::SourceStream;

pub struct EmptyScanSource {
    schema: SchemaRef,
}

impl EmptyScanSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for EmptyScanSource {
    #[instrument(name = "EmptyScanSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        let empty = Arc::new(MicroPartition::empty(Some(self.schema.clone())));
        Ok(Box::pin(futures::stream::once(async { Ok(empty) })))
    }
    fn name(&self) -> &'static str {
        "EmptyScanSource"
    }
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
