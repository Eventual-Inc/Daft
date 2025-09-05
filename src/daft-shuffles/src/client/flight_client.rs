use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_flight::{Ticket, client::FlightClient, decode::FlightRecordBatchStream};
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::SchemaRef, series::Series};
use daft_recordbatch::RecordBatch;
use daft_schema::field::FieldRef;
use futures::{FutureExt, Stream, StreamExt};
use tonic::transport::Endpoint;

enum ClientState {
    // The address of the flight server
    Uninitialized(String),
    // The address of the flight server and the flight client
    Initialized(String, Box<FlightClient>),
}

pub struct ShuffleFlightClient {
    inner: ClientState,
    schema: SchemaRef,
}

impl ShuffleFlightClient {
    pub fn new(address: String, schema: SchemaRef) -> Self {
        Self {
            inner: ClientState::Uninitialized(address),
            schema,
        }
    }

    async fn connect(&mut self) -> DaftResult<(&str, &mut FlightClient)> {
        if let ClientState::Uninitialized(address) = &mut self.inner {
            let endpoint = Endpoint::from_shared(address.clone()).map_err(|e| {
                DaftError::External(format!("Failed to create endpoint: {}", e).into())
            })?;
            let channel = endpoint.connect().await.map_err(|e| {
                DaftError::External(format!("Failed to connect to endpoint: {}", e).into())
            })?;
            let client = FlightClient::new(channel);
            let inner = client.into_inner().max_decoding_message_size(usize::MAX);
            self.inner = ClientState::Initialized(
                std::mem::take(address),
                Box::new(FlightClient::new_from_inner(inner)),
            );
        }
        match &mut self.inner {
            ClientState::Uninitialized(_) => unreachable!("Client should be initialized"),
            ClientState::Initialized(address, client) => Ok((address, client)),
        }
    }

    pub async fn get_partition(
        &mut self,
        partition_idx: usize,
    ) -> DaftResult<FlightRecordBatchStreamToDaftRecordBatchStream> {
        let ticket = Ticket::new(partition_idx.to_string());
        let (address, client) = self.connect().await?;
        let stream = client.do_get(ticket).await.map_err(|e| {
            DaftError::External(
                format!(
                    "Error fetching partition: {} from {}. {}",
                    partition_idx, address, e
                )
                .into(),
            )
        })?;
        Ok(FlightRecordBatchStreamToDaftRecordBatchStream {
            stream,
            done: false,
            schema: self.schema.clone(),
            fields: self
                .schema
                .fields()
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect(),
        })
    }
}

pub struct FlightRecordBatchStreamToDaftRecordBatchStream {
    stream: FlightRecordBatchStream,
    done: bool,
    schema: SchemaRef,
    fields: Vec<FieldRef>,
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
                let columns = this
                    .fields
                    .iter()
                    .zip(batch.columns())
                    .map(|(field, array)| {
                        let arrow2_array = array.as_ref().into();
                        Series::try_from_field_and_arrow_array(field.clone(), arrow2_array)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                let rb =
                    RecordBatch::new_with_size(this.schema.clone(), columns, batch.num_rows())?;
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
