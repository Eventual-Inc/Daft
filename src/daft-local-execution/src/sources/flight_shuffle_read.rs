use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_shuffles::client::FlightClientManager;
use futures::stream::BoxStream;
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::pipeline::NodeName;

pub struct FlightShuffleReadSource {
    shuffle_id: u64,
    partition_idx: usize,
    server_addresses: Vec<String>,
    schema: SchemaRef,
}

impl FlightShuffleReadSource {
    pub fn new(
        shuffle_id: u64,
        partition_idx: usize,
        server_addresses: Vec<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            shuffle_id,
            partition_idx,
            server_addresses,
            schema,
        }
    }
}

#[async_trait]
impl Source for FlightShuffleReadSource {
    fn name(&self) -> NodeName {
        "FlightShuffleRead".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("FlightShuffleRead: Shuffle ID {}", self.shuffle_id),
            format!("Partition: {}", self.partition_idx),
            format!("Servers: {}", self.server_addresses.join(", ")),
        ]
    }

    #[instrument(skip_all, name = "FlightShuffleReadSource::get_data")]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let shuffle_id = self.shuffle_id;
        let partition_idx = self.partition_idx;
        let server_addresses = self.server_addresses.clone();

        // Get the global flight client manager
        let schema = self.schema.clone();
        let io_runtime = common_runtime::get_io_runtime(true);
        let server_addresses_clone = server_addresses.clone();
        let schema_clone = schema.clone();
        let micropartition = io_runtime
            .spawn(async move {
                let client_manager = FlightClientManager::get_or_create_global(
                    server_addresses_clone.clone(),
                    4, // num_parallel_fetches
                    schema_clone,
                )
                .await;

                client_manager.add_addresses(server_addresses_clone).await?;

                // Fetch the partition data from the flight server
                let micropartition = client_manager
                    .fetch_partition_with_shuffle_id(shuffle_id, partition_idx)
                    .await?;
                DaftResult::Ok(micropartition)
            })
            .await??;
        let stream: BoxStream<DaftResult<Arc<MicroPartition>>> =
            Box::pin(futures::stream::once(async move { Ok(micropartition) }));

        Ok(stream)
    }
}
