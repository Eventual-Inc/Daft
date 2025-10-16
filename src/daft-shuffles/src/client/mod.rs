pub mod flight_client;

use std::{collections::HashMap, sync::OnceLock};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::Mutex;

use crate::client::flight_client::ShuffleFlightClient;

pub struct FlightClientManager {
    clients: Mutex<HashMap<String, ShuffleFlightClient>>,
}

static FLIGHT_CLIENT_MANAGER: OnceLock<FlightClientManager> = OnceLock::new();

impl FlightClientManager {
    /// Get or create the global FlightClientManager instance
    pub fn get_or_create(addresses: Vec<String>) -> &'static Self {
        FLIGHT_CLIENT_MANAGER.get_or_init(|| {
            let mut clients = HashMap::new();
            for address in addresses {
                clients.insert(address.clone(), ShuffleFlightClient::new(address));
            }
            Self {
                clients: Mutex::new(clients),
            }
        })
    }

    /// Add new addresses to the client manager
    pub async fn add_addresses(&self, addresses: Vec<String>) {
        let mut clients = self.clients.lock().await;
        for address in addresses {
            if !clients.contains_key(&address) {
                clients.insert(address.clone(), ShuffleFlightClient::new(address));
            }
        }
    }

    pub async fn fetch_partition(
        &self,
        shuffle_id: u64,
        partition: usize,
        server_cache_mapping: &HashMap<String, Vec<u32>>,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let remote_streams = {
            let mut clients = self.clients.lock().await;
            let mut futures = Vec::new();
            for (address, client) in clients.iter_mut() {
                let cache_ids = server_cache_mapping
                    .get(address)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                futures.push(client.get_partition(
                    shuffle_id,
                    partition,
                    cache_ids,
                    schema.clone(),
                ));
            }
            futures::future::try_join_all(futures).await?
        };
        let record_batches =
            futures::stream::iter(remote_streams.into_iter()).flatten_unordered(None);
        Ok(record_batches.boxed())
    }
}
