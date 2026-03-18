use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::combine_stream;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_shuffles::{client::FlightClientManager, server::flight_server::ShuffleFlightServer};
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{channel::create_channel, pipeline::NodeName};

pub struct FlightShuffleReadSource {
    shuffle_id: u64,
    partition_idx: usize,
    server_addresses: Vec<String>,
    server_cache_mapping: HashMap<String, Vec<u32>>,
    schema: SchemaRef,
    local_server: Arc<ShuffleFlightServer>,
}

impl FlightShuffleReadSource {
    pub fn new(
        shuffle_id: u64,
        partition_idx: usize,
        server_addresses: Vec<String>,
        server_cache_mapping: HashMap<String, Vec<u32>>,
        schema: SchemaRef,
        local_server: Arc<ShuffleFlightServer>,
    ) -> Self {
        Self {
            shuffle_id,
            partition_idx,
            server_addresses,
            server_cache_mapping,
            schema,
            local_server,
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
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let shuffle_id = self.shuffle_id;
        let partition_idx = self.partition_idx;
        let server_addresses = self.server_addresses.clone();
        let server_cache_mapping = self.server_cache_mapping.clone();
        let schema = self.schema.clone();
        let local_server = self.local_server.clone();

        let io_runtime = common_runtime::get_io_runtime(true);
        let (tx, rx) = create_channel(1);
        let task = io_runtime
            .spawn(async move {
                let local_address = &local_server.ip_address;

                let (has_local, remote_addresses) = if server_addresses.contains(local_address) {
                    (
                        true,
                        server_addresses
                            .iter()
                            .filter(|a| a.as_str() != local_address)
                            .cloned()
                            .collect(),
                    )
                } else {
                    (false, server_addresses)
                };

                let local_cache_ids = server_cache_mapping.get(local_address).cloned();
                let remote_server_cache_mapping: HashMap<String, Vec<u32>> = server_cache_mapping
                    .into_iter()
                    .filter(|(addr, _)| addr.as_str() != local_address)
                    .collect();

                // Build optional local stream (in-process) and remote stream (gRPC), then merge.
                let mut client_manager = FlightClientManager::new(remote_addresses);
                let remote_stream = client_manager
                    .fetch_partition(
                        shuffle_id,
                        partition_idx,
                        &remote_server_cache_mapping,
                        schema.clone(),
                    )
                    .await?;

                let local_stream = futures::stream::iter(if has_local {
                    local_server
                        .get_partition_local(
                            shuffle_id,
                            partition_idx,
                            local_cache_ids.as_deref(),
                        )
                        .await?
                } else {
                    vec![]
                });

                let mut combined_stream = tokio_stream::StreamExt::merge(local_stream, remote_stream);

                while let Some(batch) = combined_stream.next().await {
                    let mp = MicroPartition::new_loaded(schema.clone(), vec![batch?].into(), None);
                    if tx.send(Arc::new(mp)).await.is_err() {
                        break;
                    }
                }
                DaftResult::Ok(())
            })
            .map(|result| match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e.into()),
                Err(e) => Err(e.into()),
            });

        Ok(combine_stream(rx.into_stream().map(Ok).boxed(), task).boxed())
    }
}
