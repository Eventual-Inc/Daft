use std::sync::Arc;

use arrow_flight::{
    Ticket,
    client::FlightClient,
    decode::{DecodedPayload, FlightRecordBatchStream},
};
use async_stream::try_stream;
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::SchemaRef, series::Series};
use daft_recordbatch::RecordBatch;
use daft_schema::field::FieldRef;
use futures::{StreamExt, stream::BoxStream};
use tonic::transport::Endpoint;

const PARTITION_BOUNDARY_PREFIX: &str = "partition_ref_id:";

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
        let fields = schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.clone()))
            .collect::<Vec<_>>();

        Ok(decode_batch_stream(stream, partition_ref_ids.to_vec(), schema, fields).boxed())
    }
}

fn decode_batch_stream(
    stream: FlightRecordBatchStream,
    expected_partition_ref_ids: Vec<u64>,
    schema: SchemaRef,
    fields: Vec<FieldRef>,
) -> impl futures::Stream<Item = DaftResult<RecordBatch>> + Send + 'static {
    try_stream! {
        let mut decoded_stream = stream.into_inner();
        let mut expected_partition_ref_ids = expected_partition_ref_ids.into_iter();
        let mut current_partition_ref_id = None;

        while let Some(message) = decoded_stream.next().await {
            let message = message.map_err(|e| DaftError::External(e.to_string().into()))?;
            match message.payload {
                DecodedPayload::Schema(_) => {}
                DecodedPayload::None => {
                    let Some(partition_ref_id) = parse_partition_boundary(message.app_metadata())? else {
                        continue;
                    };

                    let expected_partition_ref_id = expected_partition_ref_ids
                        .next()
                        .ok_or_else(|| DaftError::InternalError(format!(
                            "Received unexpected partition ref boundary {}",
                            partition_ref_id
                        )))?;
                    if partition_ref_id != expected_partition_ref_id {
                        Err(DaftError::InternalError(format!(
                            "Received partition ref boundary {} but expected {}",
                            partition_ref_id, expected_partition_ref_id
                        )))?;
                    }

                    current_partition_ref_id = Some(partition_ref_id);
                }
                DecodedPayload::RecordBatch(batch) => {
                    if current_partition_ref_id.is_none() {
                        Err(DaftError::InternalError(
                            "Received shuffle record batch before partition boundary".to_string(),
                        ))?;
                    }
                    let columns = fields
                        .iter()
                        .zip(batch.columns())
                        .map(|(field, array)| Series::from_arrow(field.clone(), array.clone()))
                        .collect::<DaftResult<Vec<_>>>()?;
                    yield RecordBatch::new_with_size(
                        schema.clone(),
                        columns,
                        batch.num_rows(),
                    )?;
                }
            }
        }

        if let Some(missing_partition_ref_id) = expected_partition_ref_ids.next() {
            Err(DaftError::InternalError(format!(
                "Missing partition ref boundary for {}",
                missing_partition_ref_id
            )))?;
        }
    }
}

fn parse_partition_boundary(app_metadata: impl AsRef<[u8]>) -> DaftResult<Option<u64>> {
    let app_metadata = app_metadata.as_ref();
    if app_metadata.is_empty() {
        return Ok(None);
    }

    let app_metadata = std::str::from_utf8(app_metadata).map_err(|e| {
        DaftError::InternalError(format!("Invalid shuffle partition boundary metadata: {e}"))
    })?;
    let Some(partition_ref_id) = app_metadata.strip_prefix(PARTITION_BOUNDARY_PREFIX) else {
        return Ok(None);
    };
    let partition_ref_id = partition_ref_id.parse::<u64>().map_err(|e| {
        DaftError::InternalError(format!(
            "Invalid partition ref id in boundary metadata: {e}"
        ))
    })?;
    Ok(Some(partition_ref_id))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array},
        record_batch::RecordBatch as ArrowRecordBatch,
    };
    use arrow_flight::{
        FlightData, decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder,
    };
    use arrow_ipc::{MessageArgs, MessageHeader, MetadataVersion, finish_message_buffer};
    use daft_core::prelude::{DataType, Field, Schema};
    use flatbuffers::FlatBufferBuilder;
    use futures::{StreamExt, TryStreamExt, stream};

    use super::*;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
    }

    fn make_fields() -> Vec<FieldRef> {
        make_schema()
            .fields()
            .iter()
            .map(|field| Arc::new(field.clone()))
            .collect()
    }

    fn make_batch() -> ArrowRecordBatch {
        ArrowRecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
        )])
        .unwrap()
    }

    fn partition_boundary(partition_ref_id: u64) -> FlightData {
        let mut builder = FlatBufferBuilder::new();
        let message = arrow_ipc::Message::create(
            &mut builder,
            &MessageArgs {
                version: MetadataVersion::V5,
                header_type: MessageHeader::NONE,
                header: None,
                bodyLength: 0,
                custom_metadata: None,
            },
        );
        finish_message_buffer(&mut builder, message);
        FlightData {
            data_header: builder.finished_data().to_vec().into(),
            app_metadata: format!("partition_ref_id:{partition_ref_id}").into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn decode_batch_stream_rejects_record_batch_before_boundary() {
        let schema = make_schema();
        let stream = FlightRecordBatchStream::new_from_flight_data(
            FlightDataEncoderBuilder::new().build(stream::iter(vec![Ok(make_batch())])),
        );

        let err = decode_batch_stream(stream, vec![11], schema, make_fields())
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Received shuffle record batch before partition boundary")
        );
    }

    #[tokio::test]
    async fn decode_batch_stream_rejects_missing_partition_boundary() {
        let schema = make_schema();
        let encoded = FlightDataEncoderBuilder::new().build(stream::iter(vec![Ok(make_batch())]));
        let stream = FlightRecordBatchStream::new_from_flight_data(
            stream::once(async { Ok(partition_boundary(11)) }).chain(encoded),
        );

        let err = decode_batch_stream(stream, vec![11, 12], schema, make_fields())
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Missing partition ref boundary for 12")
        );
    }

    #[test]
    fn parse_partition_boundary_rejects_invalid_partition_ref_id() {
        let err = parse_partition_boundary("partition_ref_id:not-a-number").unwrap_err();
        assert!(
            err.to_string()
                .contains("Invalid partition ref id in boundary metadata")
        );
    }
}
