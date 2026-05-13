use std::{
    io::{ErrorKind, SeekFrom},
    sync::atomic::Ordering,
    time::Instant,
};

use arrow_flight::FlightData;
use common_error::{DaftError, DaftResult};
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use super::flight_server::read_agg;

/// Reading state maintenance
struct ReadState<R> {
    // The reader to read from
    reader: R,
    /// When the previous FlightData was yielded. Used to attribute time spent
    /// outside `process_next` (gRPC encode + send + backpressure) to a counter
    /// separate from the disk-read time inside `process_next`.
    last_yield: Option<Instant>,
}

/// State machine for stream processing
enum StreamState<R> {
    // A tuple of the current read state and the flight data to be sent
    Ready((ReadState<R>, FlightData)),
    // The read state is done, no more data to read
    Done,
}

const CONTINUATION_MARKER: i32 = -1;

/// A reader that reads arrow ipc files in stream format and converts them to FlightData for serving
/// over flight. This is an optimization where we skip converting the ipc files to RecordBatches
/// and instead read the data directly into FlightData, since we already know that the data is in
/// arrow ipc stream format.
pub struct FlightDataStreamReader<R: AsyncRead + Unpin> {
    state: Option<ReadState<R>>,
}

impl<R: AsyncRead + AsyncSeek + Unpin> FlightDataStreamReader<R> {
    pub async fn try_new(mut reader: R) -> DaftResult<Self> {
        // Skip stream metadata in the file since we don't need it when sending data over flight
        skip_stream_metadata(&mut reader).await?;
        Ok(Self {
            state: Some(ReadState { reader, last_yield: None }),
        })
    }
}

impl<R: AsyncRead + Unpin> FlightDataStreamReader<R> {
    /// Construct from a reader that has already been positioned past the IPC stream metadata
    /// (schema header). Used for ranged reads where the reader is a `Take<File>` and the file
    /// has already been seeked to a batch boundary.
    pub fn from_skipped(reader: R) -> Self {
        Self {
            state: Some(ReadState { reader, last_yield: None }),
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = DaftResult<FlightData>> {
        futures::stream::unfold(self.state, |state| async move {
            let current = state?;
            match process_next(current).await {
                Ok(StreamState::Ready((next_state, data))) => Some((Ok(data), Some(next_state))),
                Ok(StreamState::Done) => None,
                Err(e) => Some((Err(e), None)),
            }
        })
    }
}

/// Skip stream metadata on reader. We don't need it when sending data over flight.
pub async fn skip_stream_metadata<R: AsyncRead + AsyncSeek + Unpin>(
    reader: &mut R,
) -> DaftResult<()> {
    let mut meta_len = reader.read_i32_le().await?;
    if meta_len == CONTINUATION_MARKER {
        meta_len = reader.read_i32_le().await?;
    }

    let meta_len: u64 = meta_len
        .try_into()
        .map_err(|_| arrow_schema::ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    reader.seek(SeekFrom::Current(meta_len as i64)).await?;
    Ok(())
}

/// Process next IPC message into FlightData
async fn process_next<R: AsyncRead + Unpin>(mut state: ReadState<R>) -> DaftResult<StreamState<R>> {
    // Time since last yield = framework overhead between two consecutive
    // FlightData emissions (gRPC encode/send/backpressure). First call (no
    // previous yield) is not counted.
    if let Some(prev) = state.last_yield.take() {
        read_agg::SEND_GAP_US
            .fetch_add(prev.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    // Phase A: meta_len + header read.
    let t_header = Instant::now();
    let mut meta_len = match state.reader.read_i32_le().await {
        Ok(meta_len) => meta_len,
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
            // TODO: Should we return an error here?
            return Ok(StreamState::Done);
        }
        Err(e) => return Err(DaftError::from(e)),
    };

    if meta_len == CONTINUATION_MARKER {
        meta_len = state.reader.read_i32_le().await?;
    }

    let meta_len: usize = meta_len
        .try_into()
        .map_err(|_| arrow_schema::ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    if meta_len == 0 {
        return Ok(StreamState::Done);
    }

    // Read message header
    let mut message_buffer = vec![0; meta_len];
    state.reader.read_exact(&mut message_buffer).await?;

    // Read message body length
    let message = arrow_ipc::root_as_message(&message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;
    read_agg::MSG_HEADER_READ_US
        .fetch_add(t_header.elapsed().as_micros() as u64, Ordering::Relaxed);

    // Phase B: body read.
    let t_body = Instant::now();
    let mut data_buffer = vec![0; body_length];
    state.reader.read_exact(&mut data_buffer).await?;
    read_agg::MSG_BODY_READ_US
        .fetch_add(t_body.elapsed().as_micros() as u64, Ordering::Relaxed);

    let flight_data = FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    };

    read_agg::FLIGHTDATAS_EMITTED.fetch_add(1, Ordering::Relaxed);
    state.last_yield = Some(Instant::now());

    Ok(StreamState::Ready((state, flight_data)))
}
