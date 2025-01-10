use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::{partitioning::PartitionSetRef, MicroPartitionRef};
use tracing::instrument;

use super::source::Source;
use crate::sources::source::SourceStream;

pub struct InMemorySource {
    data: Option<PartitionSetRef<MicroPartitionRef>>,
    size_bytes: usize,
    schema: SchemaRef,
}

impl InMemorySource {
    pub fn new(
        data: Option<PartitionSetRef<MicroPartitionRef>>,
        schema: SchemaRef,
        size_bytes: usize,
    ) -> Self {
        Self {
            data,
            size_bytes,
            schema,
        }
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
        Ok(self
            .data
            .as_ref()
            .unwrap_or_else(|| panic!("No data in InMemorySource"))
            .clone()
            .to_partition_stream())
    }
    fn name(&self) -> &'static str {
        "InMemorySource"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("InMemorySource:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.size_bytes));
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
