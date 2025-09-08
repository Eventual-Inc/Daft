pub mod flight_client;

use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::{Mutex, OnceCell, Semaphore};

use crate::client::flight_client::ShuffleFlightClient;

pub struct FlightClientManager {
    clients: Mutex<HashMap<String, ShuffleFlightClient>>,
    semaphore: Semaphore,
    schema: SchemaRef,
}

static GLOBAL_CLIENT_MANAGER: OnceCell<Arc<FlightClientManager>> = OnceCell::const_new();

impl FlightClientManager {
    pub fn new(addresses: Vec<String>, num_parallel_fetches: usize, schema: SchemaRef) -> Self {
        let mut clients = HashMap::new();
        for address in addresses {
            clients.insert(
                address.clone(),
                ShuffleFlightClient::new(address),
            );
        }
        Self {
            clients: Mutex::new(clients),
            semaphore: Semaphore::new(num_parallel_fetches),
            schema,
        }
    }

    pub async fn get_or_create_global(
        addresses: Vec<String>,
        num_parallel_fetches: usize,
        schema: SchemaRef,
    ) -> Arc<Self> {
        GLOBAL_CLIENT_MANAGER
            .get_or_init(|| async { Arc::new(Self::new(addresses, num_parallel_fetches, schema)) })
            .await
            .clone()
    }

    pub async fn add_addresses(&self, addresses: Vec<String>) -> DaftResult<()> {
        let mut clients = self.clients.lock().await;
        for address in addresses {
            if !clients.contains_key(&address) {
                clients.insert(
                    address.clone(),
                    ShuffleFlightClient::new(address),
                );
            }
        }
        Ok(())
    }

    pub async fn fetch_partition(&self, partition: usize) -> DaftResult<Arc<MicroPartition>> {
        let io_runtime = get_io_runtime(true);
        let permit =
            self.semaphore.acquire().await.map_err(|e| {
                DaftError::InternalError(format!("Failed to acquire semaphore: {}", e))
            })?;
        let remote_streams = {
            let mut clients = self.clients.lock().await;
            futures::future::try_join_all(
                clients
                    .values_mut()
                    .map(|client| client.get_partition(partition)),
            )
            .await?
        };

        let schema = self.schema.clone();
        let res = io_runtime
            .spawn(async move {
                let record_batches = futures::stream::iter(remote_streams.into_iter())
                    .flatten_unordered(None)
                    .try_collect::<Vec<_>>()
                    .await?;
                let mp = MicroPartition::new_loaded(schema, record_batches.into(), None);
                DaftResult::Ok(Arc::new(mp))
            })
            .await??;

        drop(permit);
        Ok(res)
    }

    pub async fn fetch_partition_with_shuffle_id(
        &self,
        shuffle_id: u64,
        partition: usize,
        schema: SchemaRef,
    ) -> DaftResult<impl Stream<Item = DaftResult<RecordBatch>>> {
        let _permit =
            self.semaphore.acquire().await.map_err(|e| {
                DaftError::InternalError(format!("Failed to acquire semaphore: {}", e))
            })?;
        let remote_streams = {
            let mut clients = self.clients.lock().await;
            futures::future::try_join_all(
                clients
                    .values_mut()
                    .map(|client| client.get_partition_with_shuffle_id(shuffle_id, partition, schema.clone())),
            )
            .await?
        };
        let record_batches = futures::stream::iter(remote_streams.into_iter())
            .flatten_unordered(None);
        Ok(record_batches)
    }
}
