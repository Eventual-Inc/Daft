use std::sync::Arc;

use async_stream::try_stream;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_table::Table;
use tracing::instrument;

use super::source::Source;
use crate::{sources::source::SourceStream, ExecutionRuntimeHandle};

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
            let empty = vec![Table::empty(Some(self.schema.clone())); 1];
            return Ok(Box::pin(futures::stream::once(async {
                Ok(Arc::new(empty))
            })));
        }
        let data = self.data.clone();
        let stream = try_stream! {
            for mp in data {
                yield mp.get_tables()?
            }
        };
        Ok(Box::pin(stream))
    }
    fn name(&self) -> &'static str {
        "InMemory"
    }
}
