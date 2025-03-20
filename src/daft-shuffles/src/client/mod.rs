pub mod flight_client;

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use flight_client::FlightRecordBatchStreamToDaftRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::client::flight_client::ShuffleFlightClient;

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
