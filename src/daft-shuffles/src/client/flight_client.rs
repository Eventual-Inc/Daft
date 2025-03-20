use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow_flight::{client::FlightClient, decode::FlightRecordBatchStream, Ticket};
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::Schema, series::Series};
use daft_recordbatch::RecordBatch;
use daft_schema::field::Field;
use futures::{FutureExt, Stream, StreamExt};
use tonic::transport::Channel;

pub struct ShuffleFlightClient {
    inner: FlightClient,
    address: String,
}

impl ShuffleFlightClient {
    pub async fn new(address: String) -> Self {
        let endpoint = Channel::from_shared(address.clone()).unwrap();
        let channel = endpoint.connect().await.unwrap();
        Self {
            inner: FlightClient::new(channel),
            address,
        }
    }

    pub async fn get_partition(
        &mut self,
        partition_idx: usize,
    ) -> DaftResult<FlightRecordBatchStreamToDaftRecordBatchStream> {
        let ticket = Ticket::new(format!("{}", partition_idx));
        let stream = self.inner.do_get(ticket).await.map_err(|e| {
            DaftError::External(
                format!(
                    "Error fetching partition: {} from {}. {}",
                    partition_idx, self.address, e
                )
                .into(),
            )
        })?;
        Ok(FlightRecordBatchStreamToDaftRecordBatchStream {
            stream,
            done: false,
        })
    }
}

pub struct FlightRecordBatchStreamToDaftRecordBatchStream {
    stream: FlightRecordBatchStream,
    done: bool,
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
                let columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        let col_name = field.name();
                        let arrow_array = batch
                            .column_by_name(col_name)
                            .expect("Column should exist in RecordBatch");
                        let arrow2_array = arrow_array.as_ref().into();
                        Series::try_from((col_name.as_str(), arrow2_array))
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                let rb = RecordBatch::from_nonempty_columns(columns);
                Poll::Ready(Some(rb))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(DaftError::External(e.to_string().into()))))
            }
            Poll::Ready(None) => {
                // Mark as done and return an empty record batch
                this.done = true;

                // Create an empty record batch with the same schema
                let fields = this
                    .stream
                    .schema()
                    .expect("Schema should exist once stream is done")
                    .fields()
                    .iter()
                    .map(|field| {
                        let arrow2_field = field.into();
                        let daft_field = Field::from(&arrow2_field);
                        Ok(daft_field)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                let empty_batch = RecordBatch::empty(Some(Schema::new(fields)?.into()));
                Poll::Ready(Some(empty_batch))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
