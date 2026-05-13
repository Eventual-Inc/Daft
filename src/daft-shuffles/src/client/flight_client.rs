use std::{
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    time::Instant,
};

use arrow_flight::{Ticket, client::FlightClient, decode::FlightRecordBatchStream};
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::SchemaRef, series::Series};
use daft_recordbatch::RecordBatch;
use daft_schema::field::FieldRef;
use futures::{FutureExt, Stream, StreamExt};
use tonic::transport::Endpoint;

use crate::server::flight_server::read_agg;

#[allow(clippy::large_enum_variant)]
enum ClientState {
    // The address of the flight server
    Uninitialized(String),
    // The address of the flight server and the flight client
    Initialized(String, FlightClient),
}

pub struct ShuffleFlightClient {
    inner: ClientState,
}

impl ShuffleFlightClient {
    pub fn new(address: String) -> Self {
        Self {
            inner: ClientState::Uninitialized(address),
        }
    }

    async fn connect(&mut self) -> DaftResult<(&str, &mut FlightClient)> {
        if let ClientState::Uninitialized(address) = &mut self.inner {
            let endpoint = Endpoint::from_shared(address.clone()).map_err(|e| {
                DaftError::External(format!("Failed to create endpoint: {:?}", e).into())
            })?;
            let channel = endpoint.connect().await.map_err(|e| {
                DaftError::External(format!("Failed to connect to endpoint: {:?}", e).into())
            })?;
            let client = FlightClient::new(channel);
            let inner = client.into_inner().max_decoding_message_size(usize::MAX);
            self.inner = ClientState::Initialized(
                std::mem::take(address),
                FlightClient::new_from_inner(inner),
            );
        }
        match &mut self.inner {
            ClientState::Uninitialized(_) => unreachable!("Client should be initialized"),
            ClientState::Initialized(address, client) => Ok((address, client)),
        }
    }

    /// Get a stream of RecordBatches from a remote shuffle server
    /// Note, this function should not take long, since the actual data transfer is done in the stream.
    pub async fn get_partition(
        &mut self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
        schema: SchemaRef,
    ) -> DaftResult<FlightRecordBatchStreamToDaftRecordBatchStream> {
        let ticket = Ticket::new(format!(
            "{}:{}",
            shuffle_id,
            partition_ref_ids
                .iter()
                .map(|partition_ref_id| partition_ref_id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
        let (address, client) = self.connect().await?;
        let stream = client.do_get(ticket).await.map_err(|e| {
            DaftError::External(
                format!(
                    "Error fetching partition refs {:?} from shuffle {} at {}. {}",
                    partition_ref_ids, shuffle_id, address, e
                )
                .into(),
            )
        })?;
        Ok(FlightRecordBatchStreamToDaftRecordBatchStream::new(
            stream, schema,
        ))
    }
}

pub struct FlightRecordBatchStreamToDaftRecordBatchStream {
    stream: FlightRecordBatchStream,
    done: bool,
    schema: SchemaRef,
    fields: Vec<FieldRef>,
    /// When the previous Ready(batch) was yielded. The gap from here to the
    /// next Ready(batch) covers: client-side gRPC wait + Flight IPC decode
    /// (inside `stream.next()`). One sample per delivered batch.
    last_yield: Option<Instant>,
}

impl FlightRecordBatchStreamToDaftRecordBatchStream {
    pub fn new(stream: FlightRecordBatchStream, schema: SchemaRef) -> Self {
        Self {
            stream,
            done: false,
            schema: schema.clone(),
            fields: schema
                .fields()
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect(),
            last_yield: None,
        }
    }
}

impl Stream for FlightRecordBatchStreamToDaftRecordBatchStream {
    type Item = DaftResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.done {
            return Poll::Ready(None);
        }

        let batch = this.stream.next().poll_unpin(cx);
        match batch {
            Poll::Ready(Some(Ok(batch))) => {
                // Time-to-next-batch on the client side (includes the gRPC
                // wait + Flight IPC decode that arrow-flight does inside
                // `stream.next()`).
                if let Some(prev) = this.last_yield.take() {
                    read_agg::CLIENT_BATCH_DELIVERY_US
                        .fetch_add(prev.elapsed().as_micros() as u64, Ordering::Relaxed);
                }
                let t_conv = Instant::now();
                let columns = this
                    .fields
                    .iter()
                    .zip(batch.columns())
                    .map(|(field, array)| Series::from_arrow(field.clone(), array.clone()))
                    .collect::<DaftResult<Vec<_>>>()?;
                let rb =
                    RecordBatch::new_with_size(this.schema.clone(), columns, batch.num_rows())?;
                read_agg::CLIENT_CONVERT_US
                    .fetch_add(t_conv.elapsed().as_micros() as u64, Ordering::Relaxed);
                read_agg::CLIENT_BATCHES_RECEIVED.fetch_add(1, Ordering::Relaxed);
                this.last_yield = Some(Instant::now());
                Poll::Ready(Some(Ok(rb)))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(DaftError::External(e.to_string().into()))))
            }
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
