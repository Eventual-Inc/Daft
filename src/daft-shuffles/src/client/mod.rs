pub mod flight_client;

use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::{get_io_runtime, RuntimeTask};
use daft_micropartition::MicroPartition;
use flight_client::FlightRecordBatchStreamToDaftRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
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
pub struct ShuffleFlightClientManager {
    client_state: Mutex<ClientState>,
    semaphore: Arc<Semaphore>,
}

impl ShuffleFlightClientManager {
    pub fn new(addresses: Vec<String>, num_parallel_fetches: usize) -> Self {
        let client_state = ClientState::Uninitialized(addresses);
        Self {
            client_state: Mutex::new(client_state),
            semaphore: Arc::new(Semaphore::new(num_parallel_fetches)),
        }
    }

    pub async fn fetch_partition(&self, partition: usize) -> DaftResult<Arc<MicroPartition>> {
        let io_runtime = get_io_runtime(true);
        let semaphore = self.semaphore.clone();
        let permit = semaphore.acquire_owned().await.unwrap();
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
                drop(permit);
                DaftResult::Ok(Arc::new(mp))
            })
            .await??;

        Ok(res)
    }
}

fn get_streams_from_client(
    client: ShuffleFlightClient,
    partitions: Vec<usize>,
) -> impl Stream<Item = DaftResult<FlightRecordBatchStreamToDaftRecordBatchStream>> {
    let partition_iter = partitions.into_iter();
    let stream = futures::stream::try_unfold(
        (client, partition_iter),
        |(mut client, mut partition_iter)| async move {
            let partition = match partition_iter.next() {
                Some(partition) => partition,
                None => {
                    // drop client
                    drop(client);
                    return Ok(None);
                }
            };
            let stream = client.get_partition(partition).await?;
            Ok(Some((stream, (client, partition_iter))))
        },
    );
    stream
}

pub async fn fetch_partitions_from_flight(
    addresses: Vec<String>,
    partitions: Vec<usize>,
    num_parallel_partitions: usize,
) -> DaftResult<impl Stream<Item = DaftResult<MicroPartition>>> {
    let clients = futures::future::join_all(
        addresses
            .into_iter()
            .map(|address| ShuffleFlightClient::new(address)),
    )
    .await;

    let stream_iters = clients
        .into_iter()
        .map(|client| Box::pin(get_streams_from_client(client, partitions.clone())))
        .collect::<Vec<_>>();
    let result_stream = futures::stream::try_unfold(stream_iters, |mut stream_iters| async move {
        let record_batch_streams =
            futures::future::join_all(stream_iters.iter_mut().map(|stream| stream.next()))
                .await
                .into_iter()
                .filter_map(|result| result)
                .collect::<DaftResult<Vec<_>>>()?;
        if record_batch_streams.is_empty() {
            return Ok(None);
        }
        if record_batch_streams.len() != stream_iters.len() {
            panic!("Some clients failed to fetch partitions");
        }
        Ok(Some((record_batch_streams, stream_iters)))
    })
    .map(
        |record_batch_streams: DaftResult<Vec<FlightRecordBatchStreamToDaftRecordBatchStream>>| {
            get_io_runtime(true).spawn(async move {
                let record_batch_streams = record_batch_streams?;
                let record_batches = futures::stream::iter(record_batch_streams.into_iter())
                    .flatten_unordered(None)
                    .try_collect::<Vec<_>>()
                    .await?;
                let mp = MicroPartition::new_loaded(
                    record_batches[0].schema.clone(),
                    record_batches.into(),
                    None,
                );
                Ok(mp)
            })
        },
    )
    .buffered(num_parallel_partitions)
    .map(|r| match r {
        Ok(Ok(mp)) => Ok(mp),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(e),
    });
    Ok(result_stream)
}
