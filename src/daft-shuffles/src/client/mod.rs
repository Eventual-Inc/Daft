pub mod flight_client;

use std::collections::HashMap;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};

use crate::client::flight_client::ShuffleFlightClient;

pub struct FlightClientManager {
    clients: HashMap<String, ShuffleFlightClient>,
}

impl FlightClientManager {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    pub async fn fetch_partition(
        &mut self,
        shuffle_id: u64,
        partition: usize,
        server_cache_mapping: &HashMap<String, Vec<u32>>,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        // Ensure clients exist for all addresses before collecting futures
        for address in server_cache_mapping.keys() {
            self.clients
                .entry(address.clone())
                .or_insert_with(|| ShuffleFlightClient::new(address.clone()));
        }

        let mut futures = Vec::new();
        for (address, client) in &mut self.clients {
            if let Some(cache_ids) = server_cache_mapping.get(address) {
                futures.push(client.get_partition(
                    shuffle_id,
                    partition,
                    cache_ids.as_slice(),
                    schema.clone(),
                ));
            }
        }

        let remote_streams = futures::future::try_join_all(futures).await?;
        let record_batches =
            futures::stream::iter(remote_streams.into_iter()).flatten_unordered(None);
        Ok(record_batches.boxed())
    }
}

impl Default for FlightClientManager {
    fn default() -> Self {
        Self::new()
    }
}
