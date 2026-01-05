use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::{MicroPartitionRef, partitioning::PartitionSetRef};
use tracing::instrument;

use super::source::Source;
use crate::{
    plan_input::{InputId, PipelineMessage},
    pipeline::NodeName,
    sources::source::SourceStream,
};

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
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        use daft_micropartition::MicroPartition;
        use futures::StreamExt;
        const PLACEHOLDER_INPUT_ID: InputId = 0;
        let schema = self.schema.clone();

        // If no data, emit empty micropartition
        let Some(data) = &self.data else {
            let empty = Arc::new(MicroPartition::empty(Some(schema)));
            let empty_morsel = PipelineMessage::Morsel {
                input_id: PLACEHOLDER_INPUT_ID,
                partition: empty,
            };
            let stream = futures::stream::once(async move { Ok(empty_morsel) });
            return Ok(Box::pin(stream));
        };

        let stream = data.clone().to_partition_stream();
        let wrapped_stream = stream.map(move |result| {
            result.map(|partition| PipelineMessage::Morsel {
                input_id: PLACEHOLDER_INPUT_ID,
                partition: partition.into(),
            })
        });
        Ok(Box::pin(wrapped_stream))
    }

    fn name(&self) -> NodeName {
        "InMemorySource".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::InMemoryScan
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
