use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;
use crate::{pipeline::NodeName, sources::source::SourceStream};

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
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let empty = Arc::new(MicroPartition::empty(Some(self.schema.clone())));
        Ok(Box::pin(futures::stream::once(async { Ok(empty) })))
    }

    fn name(&self) -> NodeName {
        "EmptyScan".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::EmptyScan
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("EmptyScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
