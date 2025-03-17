use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{ready, Poll},
};

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::SchemaRef,
    flight::data::FlightData,
    io::{
        flight::{deserialize_batch, deserialize_dictionary, deserialize_schemas},
        ipc::{read::Dictionaries, IpcSchema},
    },
};
use arrow_flight::{error::FlightError, flight_service_client::FlightServiceClient, Ticket};
use arrow_ipc::MessageHeader;
use common_error::{DaftError, DaftResult};
use daft_core::series::Series;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use tonic::{metadata::MetadataMap, transport::Channel};

pub struct ShuffleFlightClient {
    inner: FlightServiceClient<Channel>,
}

impl ShuffleFlightClient {
    pub async fn new(address: String) -> Self {
        let endpoint = Channel::from_shared(address).unwrap();
        let channel = endpoint.connect().await.unwrap();
        Self {
            inner: FlightServiceClient::new(channel),
        }
    }

    pub async fn get_partition(
        &mut self,
        partition_idx: usize,
    ) -> DaftResult<FlightRecordBatchStream> {
        let ticket = Ticket::new(format!("{}", partition_idx));
        let (metadata_map, flight_stream, _) = self
            .inner
            .do_get(ticket)
            .await
            .map_err(|e| {
                DaftError::External(
                    format!(
                        "Flight Server error getting partition {}, error: {}",
                        partition_idx, e
                    )
                    .into(),
                )
            })?
            .into_parts();
        let stream = FlightRecordBatchStream::new_from_flight_data(
            flight_stream
                .map_err(FlightError::Tonic)
                .map_ok(|data| FlightData {
                    flight_descriptor: None,
                    data_header: data.data_header.to_vec(),
                    data_body: data.data_body.to_vec(),
                    app_metadata: data.app_metadata.to_vec(),
                }),
        )
        .with_headers(metadata_map);

        Ok(stream)
    }
}

impl Drop for ShuffleFlightClient {
    fn drop(&mut self) {
        println!("dropping client");
    }
}

pub async fn reduce_partitions(
    addresses: Vec<String>,
    partitions: Vec<usize>,
) -> DaftResult<impl Stream<Item = DaftResult<MicroPartition>>> {
    let clients = futures::future::join_all(
        addresses
            .into_iter()
            .map(|address| ShuffleFlightClient::new(address)),
    )
    .await;

    let partition_iter = partitions.into_iter();
    let stream = futures::stream::try_unfold(
        (clients, partition_iter),
        |(mut clients, mut partition_iter)| async move {
            let partition = match partition_iter.next() {
                Some(partition) => partition,
                None => {
                    // drop clients
                    drop(clients);
                    println!("dropping clients");
                    return Ok(None);
                }
            };

            let streams = futures::future::try_join_all(
                clients
                    .iter_mut()
                    .map(|client| client.get_partition(partition)),
            )
            .await?;
            let mut results = Vec::new();
            let mut schema = None;
            for mut stream in streams {
                while let Some(result) = stream.next().await {
                    results.push(result?);
                }
                if schema.is_none() {
                    schema = Some(daft_schema::schema::Schema::try_from(
                        stream.schema().unwrap().as_ref(),
                    )?);
                }
            }
            let mp = MicroPartition::new_loaded(schema.unwrap().into(), results.into(), None);
            Ok(Some((mp, (clients, partition_iter))))
        },
    );
    Ok(stream)
}

#[derive(Debug)]
pub struct FlightRecordBatchStream {
    /// Optional grpc header metadata.
    headers: MetadataMap,

    inner: FlightDataDecoder,
}

impl FlightRecordBatchStream {
    /// Create a new [`FlightRecordBatchStream`] from a decoded stream
    pub fn new(inner: FlightDataDecoder) -> Self {
        Self {
            inner,
            headers: MetadataMap::default(),
        }
    }

    /// Create a new [`FlightRecordBatchStream`] from a stream of [`FlightData`]
    pub fn new_from_flight_data<S>(inner: S) -> Self
    where
        S: Stream<Item = arrow_flight::error::Result<FlightData>> + Send + 'static,
    {
        Self {
            inner: FlightDataDecoder::new(inner),
            headers: MetadataMap::default(),
        }
    }

    /// Record response headers.
    pub fn with_headers(self, headers: MetadataMap) -> Self {
        Self { headers, ..self }
    }

    /// Headers attached to this stream.
    pub fn headers(&self) -> &MetadataMap {
        &self.headers
    }

    /// Return schema for the stream, if it has been received
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.inner.schema()
    }

    /// Consume self and return the wrapped [`FlightDataDecoder`]
    pub fn into_inner(self) -> FlightDataDecoder {
        self.inner
    }
}

impl futures::Stream for FlightRecordBatchStream {
    type Item = DaftResult<RecordBatch>;

    /// Returns the next [`RecordBatch`] available in this stream, or `None` if
    /// there are no further results available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<DaftResult<RecordBatch>>> {
        loop {
            let had_schema = self.schema().is_some();
            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                // Inner exhausted
                None => {
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(DaftError::External(e.into()))));
                }
                // translate data
                Some(Ok(data)) => match data.payload {
                    DecodedPayload::Schema(_) if had_schema => {
                        return Poll::Ready(Some(Err(DaftError::External(
                            "Unexpectedly saw multiple Schema messages in FlightData stream".into(),
                        ))));
                    }
                    DecodedPayload::Schema(_) => {
                        // Need next message, poll inner again
                    }
                    DecodedPayload::RecordBatch(batch) => {
                        let columns = batch
                            .into_arrays()
                            .into_iter()
                            .zip(&self.schema().unwrap().fields)
                            .map(|(array, field)| Series::try_from((field.name.as_str(), array)))
                            .collect::<DaftResult<Vec<_>>>()?;
                        return Poll::Ready(Some(Ok(RecordBatch::from_nonempty_columns(columns)?)));
                    }
                    DecodedPayload::None => {
                        // Need next message
                    }
                },
            }
        }
    }
}

/// Wrapper around a stream of [`FlightData`] that handles the details
/// of decoding low level Flight messages into [`Schema`] and
/// [`RecordBatch`]es, including details such as dictionaries.
///
/// # Protocol Details
///
/// The client handles flight messages as followes:
///
/// - **None:** This message has no effect. This is useful to
///   transmit metadata without any actual payload.
///
/// - **Schema:** The schema is (re-)set. Dictionaries are cleared and
///   the decoded schema is returned.
///
/// - **Dictionary Batch:** A new dictionary for a given column is registered. An existing
///   dictionary for the same column will be overwritten. This
///   message is NOT visible.
///
/// - **Record Batch:** Record batch is created based on the current
///   schema and dictionaries. This fails if no schema was transmitted
///   yet.
///
/// All other message types (at the time of writing: e.g. tensor and
/// sparse tensor) lead to an error.
///
/// Example usecases
///
/// 1. Using this low level stream it is possible to receive a steam
///    of RecordBatches in FlightData that have different schemas by
///    handling multiple schema messages separately.
pub struct FlightDataDecoder {
    /// Underlying data stream
    response: BoxStream<'static, arrow_flight::error::Result<FlightData>>,
    /// Decoding state
    state: Option<FlightStreamState>,
    /// Seen the end of the inner stream?
    done: bool,
}

impl Debug for FlightDataDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightDataDecoder")
            .field("response", &"<stream>")
            .field("state", &self.state)
            .field("done", &self.done)
            .finish()
    }
}

impl FlightDataDecoder {
    /// Create a new wrapper around the stream of [`FlightData`]
    pub fn new<S>(response: S) -> Self
    where
        S: Stream<Item = arrow_flight::error::Result<FlightData>> + Send + 'static,
    {
        Self {
            state: None,
            response: response.boxed(),
            done: false,
        }
    }

    /// Returns the current schema for this stream
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.state.as_ref().map(|state| &state.schema)
    }

    /// Extracts flight data from the next message, updating decoding
    /// state as necessary.
    fn extract_message(
        &mut self,
        data: FlightData,
    ) -> arrow_flight::error::Result<Option<DecodedFlightData>> {
        let message = arrow_ipc::root_as_message(&data.data_header[..])
            .map_err(|e| FlightError::DecodeError(format!("Error decoding root message: {e}")))?;

        match message.header_type() {
            MessageHeader::NONE => Ok(Some(DecodedFlightData::new_none(data))),
            MessageHeader::Schema => {
                let (schema, ipc_schema) = deserialize_schemas(&data.data_header[..])
                    .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;

                let schema = Arc::new(schema);
                let dictionaries = Dictionaries::new();

                self.state = Some(FlightStreamState {
                    schema: schema.clone(),
                    ipc_schema,
                    dictionaries,
                });
                Ok(Some(DecodedFlightData::new_schema(data, schema)))
            }
            MessageHeader::DictionaryBatch => {
                let state = if let Some(state) = self.state.as_mut() {
                    state
                } else {
                    return Err(FlightError::protocol(
                        "Received DictionaryBatch prior to Schema",
                    ));
                };

                deserialize_dictionary(
                    &data,
                    &state.schema.fields,
                    &state.ipc_schema,
                    &mut state.dictionaries,
                )
                .map_err(|e| FlightError::DecodeError(format!("Error decoding dictionary: {e}")))?;

                // Updated internal state, but no decoded message
                Ok(None)
            }
            MessageHeader::RecordBatch => {
                let state = if let Some(state) = self.state.as_ref() {
                    state
                } else {
                    return Err(FlightError::protocol(
                        "Received RecordBatch prior to Schema",
                    ));
                };

                let batch = deserialize_batch(
                    &data,
                    &state.schema.fields,
                    &state.ipc_schema,
                    &state.dictionaries,
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding record batch: {e}"))
                })?;

                Ok(Some(DecodedFlightData::new_record_batch(data, batch)))
            }
            other => {
                let name = other.variant_name().unwrap_or("UNKNOWN");
                Err(FlightError::protocol(format!("Unexpected message: {name}")))
            }
        }
    }
}

impl futures::Stream for FlightDataDecoder {
    type Item = arrow_flight::error::Result<DecodedFlightData>;
    /// Returns the result of decoding the next [`FlightData`] message
    /// from the server, or `None` if there are no further results
    /// available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let res = ready!(self.response.poll_next_unpin(cx));

            return Poll::Ready(match res {
                None => {
                    self.done = true;
                    None // inner is exhausted
                }
                Some(data) => Some(match data {
                    Err(e) => Err(e),
                    Ok(data) => match self.extract_message(data) {
                        Ok(Some(extracted)) => Ok(extracted),
                        Ok(None) => continue, // Need next input message
                        Err(e) => Err(e),
                    },
                }),
            });
        }
    }
}

/// tracks the state needed to reconstruct [`RecordBatch`]es from a
/// streaming flight response.
#[derive(Debug)]
struct FlightStreamState {
    schema: SchemaRef,
    ipc_schema: IpcSchema,
    dictionaries: Dictionaries,
}

/// FlightData and the decoded payload (Schema, RecordBatch), if any
#[derive(Debug)]
pub struct DecodedFlightData {
    /// The original FlightData message
    pub inner: FlightData,
    /// The decoded payload
    pub payload: DecodedPayload,
}

impl DecodedFlightData {
    /// Create a new DecodedFlightData with no payload
    pub fn new_none(inner: FlightData) -> Self {
        Self {
            inner,
            payload: DecodedPayload::None,
        }
    }

    /// Create a new DecodedFlightData with a [`Schema`] payload
    pub fn new_schema(inner: FlightData, schema: SchemaRef) -> Self {
        Self {
            inner,
            payload: DecodedPayload::Schema(schema),
        }
    }

    /// Create a new [`DecodedFlightData`] with a [`RecordBatch`] payload
    pub fn new_record_batch(inner: FlightData, batch: Chunk<Box<dyn Array>>) -> Self {
        Self {
            inner,
            payload: DecodedPayload::RecordBatch(batch),
        }
    }

    /// Return the metadata field of the inner flight data
    pub fn app_metadata(&self) -> Vec<u8> {
        self.inner.app_metadata.to_vec()
    }
}

/// The result of decoding [`FlightData`]
#[derive(Debug)]
pub enum DecodedPayload {
    /// None (no data was sent in the corresponding FlightData)
    None,

    /// A decoded Schema message
    Schema(SchemaRef),

    /// A decoded Record batch.
    RecordBatch(Chunk<Box<dyn Array>>),
}
