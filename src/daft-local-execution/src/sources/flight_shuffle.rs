use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{ops::NodeType, pipeline::NodeName};


pub struct FlightShuffleSource {
    server_address: String,
    server_port: u16,
    partition_idx: usize,
    schema: SchemaRef,
}

impl FlightShuffleSource {
    pub fn new(
        server_address: String,
        server_port: u16,
        partition_idx: usize,
        schema: SchemaRef,
    ) -> Self {
        Self {
            server_address,
            server_port,
            partition_idx,
            schema,
        }
    }
}

#[async_trait]
impl Source for FlightShuffleSource {
    #[instrument(skip_all, name = "FlightShuffleSource::get_data")]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: daft_io::IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        let _address = format!("http://{}:{}", self.server_address, self.server_port);
        let _partition_idx = self.partition_idx;
        let schema = self.schema.clone();

        // For now, return a simple stream with dummy data
        // In a real implementation, this would connect to the flight server
        let mp = MicroPartition::new_loaded(
            schema,
            Arc::new(vec![]),
            None,
        );
        
        Ok(Box::pin(futures::stream::once(async move {
            Ok(Arc::new(mp))
        })))
    }

    fn name(&self) -> NodeName {
        "FlightShuffleSource".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::InMemoryScan
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "FlightShuffleSource: partition {} from {}:{}",
            self.partition_idx, self.server_address, self.server_port
        )]
    }
}