use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::combine_stream;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_shuffles::client::FlightClientManager;
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
}

impl FlightShuffleReadSource {
    pub fn new(
        shuffle_id: u64,
        partition_idx: usize,
        server_addresses: Vec<String>,
        server_cache_mapping: HashMap<String, Vec<u32>>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            shuffle_id,
            partition_idx,
            server_addresses,
            server_cache_mapping,
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
        let server_cache_mapping = self.server_cache_mapping.clone();

        // Get the global flight client manager
        let schema = self.schema.clone();
        let io_runtime = common_runtime::get_io_runtime(true);
        let (tx, rx) = create_channel(1);
        let task = io_runtime
            .spawn(async move {
                let mut client_manager = FlightClientManager::new(server_addresses);

                // Fetch the partition data from the flight server with cache_ids
                let mut stream = client_manager
                    .fetch_partition(
                        shuffle_id,
                        partition_idx,
                        &server_cache_mapping,
                        schema.clone(),
                    )
                    .await?;
                while let Some(batch) = stream.next().await {
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
