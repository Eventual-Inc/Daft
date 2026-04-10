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
use futures::{FutureExt, Stream, StreamExt, stream::BoxStream};
use tonic::transport::Endpoint;

#[allow(clippy::large_enum_variant)]
enum ClientState {
    Uninitialized(String),
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

    pub async fn get_record_batches(
        &mut self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        if partition_ref_ids.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

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

        Ok(FlightRecordBatchStreamToDaftRecordBatchStream::new(stream, schema).boxed())
    }
}

pub struct FlightRecordBatchStreamToDaftRecordBatchStream {
    stream: FlightRecordBatchStream,
    done: bool,
    schema: SchemaRef,
    fields: Vec<FieldRef>,
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
                .map(|field| Arc::new(field.clone()))
                .collect(),
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

        match this.stream.next().poll_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let columns = this
                    .fields
                    .iter()
                    .zip(batch.columns())
                    .map(|(field, array)| Series::from_arrow(field.clone(), array.clone()))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array},
        record_batch::RecordBatch as ArrowRecordBatch,
    };
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use daft_core::prelude::{DataType, Field, Schema};
    use futures::{TryStreamExt, stream};

    use super::*;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
    }

    fn make_batch() -> ArrowRecordBatch {
        ArrowRecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
        )])
        .unwrap()
    }

    #[tokio::test]
    async fn record_batch_stream_decodes_arrow_batches() -> DaftResult<()> {
        let schema = make_schema();
        let stream = FlightDataEncoderBuilder::new().build(stream::iter(vec![Ok(make_batch())]));
        let stream = FlightRecordBatchStream::new_from_flight_data(stream);

        let batches: Vec<RecordBatch> =
            FlightRecordBatchStreamToDaftRecordBatchStream::new(stream, schema)
                .try_collect()
                .await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3);
        Ok(())
    }
}
