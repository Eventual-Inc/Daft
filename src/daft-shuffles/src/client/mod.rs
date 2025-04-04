pub mod flight_client;

use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::{Mutex, Semaphore};

use crate::client::flight_client::ShuffleFlightClient;

enum ClientState {
    Uninitialized(Vec<String>),
    Initialized(Vec<ShuffleFlightClient>),
}

impl ClientState {
    async fn get_or_create_clients(&mut self) -> &mut Vec<ShuffleFlightClient> {
        if let ClientState::Uninitialized(addresses) = self {
            let addresses = std::mem::take(addresses);
            let clients =
                futures::future::join_all(addresses.into_iter().map(ShuffleFlightClient::new))
                    .await;
            *self = ClientState::Initialized(clients);
        }
        match self {
            ClientState::Initialized(clients) => clients,
            ClientState::Uninitialized(_) => unreachable!(),
        }
    }
}
pub struct FlightClientManager {
    client_state: Mutex<ClientState>,
    semaphore: Semaphore,
}

impl FlightClientManager {
    pub fn new(addresses: Vec<String>, num_parallel_fetches: usize) -> Self {
        let client_state = ClientState::Uninitialized(addresses);
        Self {
            client_state: Mutex::new(client_state),
            semaphore: Semaphore::new(num_parallel_fetches),
        }
    }

    pub async fn fetch_partition(&self, partition: usize) -> DaftResult<Arc<MicroPartition>> {
        let io_runtime = get_io_runtime(true);
        let permit = self.semaphore.acquire().await.unwrap();
        let remote_streams = {
            let mut client_state = self.client_state.lock().await;
            let clients = client_state.get_or_create_clients().await;
            futures::future::try_join_all(
                clients
                    .iter_mut()
                    .map(|client| client.get_partition(partition)),
            )
            .await?
        };

        let res = io_runtime
            .spawn(async move {
                let record_batches = futures::stream::iter(remote_streams.into_iter())
                    .flatten_unordered(None)
                    .try_collect::<Vec<_>>()
                    .await?;
                let mp = MicroPartition::new_loaded(
                    record_batches[0].schema.clone(),
                    record_batches.into(),
                    None,
                );
                DaftResult::Ok(Arc::new(mp))
            })
            .await??;

        drop(permit);
        Ok(res)
    }
}
