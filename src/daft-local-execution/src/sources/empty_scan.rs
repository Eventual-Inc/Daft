use std::sync::Arc;

use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::{sources::source::SourceStream, ExecutionRuntimeHandle};

pub struct EmptyScanSource {
    schema: SchemaRef,
}

impl EmptyScanSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
    pub fn boxed(self) -> Box<dyn Source> {
        Box::new(self) as Box<dyn Source>
    }
}

impl Source for EmptyScanSource {
    #[instrument(name = "EmptyScanSource::get_data", level = "info", skip_all)]
    fn get_data(
        &self,
        _maintain_order: bool,
        _runtime_handle: &mut ExecutionRuntimeHandle,
        _io_stats: IOStatsRef,
    ) -> crate::Result<SourceStream<'static>> {
        let empty = Arc::new(MicroPartition::empty(Some(self.schema.clone())));
        Ok(Box::pin(futures::stream::once(async { empty })))
    }
    fn name(&self) -> &'static str {
        "EmptyScanSource"
    }
}
