pub mod flight_client;

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::{Mutex, Semaphore};

use crate::client::flight_client::ShuffleFlightClient;
pub struct FlightClientManager {
    clients: Mutex<Vec<ShuffleFlightClient>>,
    semaphore: Semaphore,
    schema: SchemaRef,
}

impl FlightClientManager {
    pub fn new(addresses: Vec<String>, num_parallel_fetches: usize, schema: SchemaRef) -> Self {
        let clients = addresses
            .into_iter()
            .map(|address| ShuffleFlightClient::new(address, schema.clone()))
            .collect();
        Self {
            clients: Mutex::new(clients),
            semaphore: Semaphore::new(num_parallel_fetches),
            schema,
        }
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
                    .iter_mut()
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
}
