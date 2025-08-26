use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use futures::{stream::BoxStream, StreamExt};
use tracing::instrument;

use super::source::Source;
use crate::{ops::NodeType, pipeline::NodeName, sources::source::SourceStream};

pub struct FlightShuffleReadSource {
    shuffle_id: String,
    partition_idx: usize,
    node_ips: Vec<String>,
    schema: SchemaRef,
}

impl FlightShuffleReadSource {
    pub fn new(
        shuffle_id: String,
        partition_idx: usize,
        node_ips: Vec<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            shuffle_id,
            partition_idx,
            node_ips,
            schema,
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for FlightShuffleReadSource {
    #[instrument(name = "FlightShuffleReadSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: Option<usize>,
    ) -> DaftResult<SourceStream<'static>> {
        let shuffle_id = self.shuffle_id.clone();
        let partition_idx = self.partition_idx;
        let node_ips = self.node_ips.clone();

        // Create a stream that fetches the partition data from flight servers
        let stream = async_stream::stream! {
            // Add node addresses to the global flight client manager
            if !node_ips.is_empty() {
                let flight_addresses: Vec<String> = node_ips
                    .iter()
                    .map(|ip| format!("grpc://{}:8080", ip)) // Default flight port
                    .collect();

                if let Err(e) = daft_shuffles::globals::add_addresses_to_client_manager(flight_addresses).await {
                    yield Err(e);
                    return;
                }
            }

            // Fetch partition using the global flight client manager
            match daft_shuffles::globals::fetch_partition_from_clients(&shuffle_id, partition_idx).await {
                Ok(partition) => yield Ok(partition),
                Err(e) => yield Err(e),
            }
        };

        Ok(Box::pin(stream) as BoxStream<'static, DaftResult<Arc<MicroPartition>>>)
    }

    fn name(&self) -> NodeName {
        format!("FlightShuffleRead[{}]", self.partition_idx).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::FileScan // Reusing FileScan as it's the closest match
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "FlightShuffleRead: partition {}",
            self.partition_idx
        ));
        res.push(format!("Schema = {}", self.schema.short_string()));
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
