//! `FlightDataStreamReader`: reads an IPC stream file's bytes asynchronously and yields
//! `FlightData` directly, skipping the `RecordBatch` decode step since the bytes are
//! already in arrow IPC stream format. Used by both the local (in-process) path and the
//! gRPC `do_get` path.

use std::io::{ErrorKind, SeekFrom};

use arrow_flight::FlightData;
use arrow_ipc::root_as_message;
use arrow_schema::ArrowError;
use common_error::{DaftError, DaftResult};
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

/// Reading state maintenance
struct ReadState<R> {
    // The reader to read from
    reader: R,
}

/// State machine for stream processing
enum StreamState<R> {
    // A tuple of the current read state and the flight data to be sent
    Ready((ReadState<R>, FlightData)),
    // The read state is done, no more data to read
    Done,
}

const CONTINUATION_MARKER: i32 = -1;

pub struct FlightDataStreamReader<R: AsyncRead + Unpin> {
    state: Option<ReadState<R>>,
}

impl<R: AsyncRead + AsyncSeek + Unpin> FlightDataStreamReader<R> {
    pub async fn try_new(mut reader: R) -> DaftResult<Self> {
        // Skip stream metadata in the file since we don't need it when sending data over flight
        skip_stream_metadata(&mut reader).await?;
        Ok(Self {
            state: Some(ReadState { reader }),
        })
    }
}

impl<R: AsyncRead + Unpin> FlightDataStreamReader<R> {
    /// Construct from a reader already positioned past the IPC schema header. Used for
    /// ranged reads where the file has been seeked to a batch boundary.
    pub fn from_skipped(reader: R) -> Self {
        Self {
            state: Some(ReadState { reader }),
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
        .map_err(|_| ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    reader.seek(SeekFrom::Current(meta_len as i64)).await?;
    Ok(())
}

/// Process next IPC message into FlightData
async fn process_next<R: AsyncRead + Unpin>(mut state: ReadState<R>) -> DaftResult<StreamState<R>> {
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
        .map_err(|_| ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    if meta_len == 0 {
        return Ok(StreamState::Done);
    }

    // Read message header
    let mut message_buffer = vec![0; meta_len];
    state.reader.read_exact(&mut message_buffer).await?;

    // Read message body length
    let message = root_as_message(&message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;

    // Read message body
    let mut data_buffer = vec![0; body_length];
    state.reader.read_exact(&mut data_buffer).await?;

    let flight_data = FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    };

    Ok(StreamState::Ready((state, flight_data)))
}
