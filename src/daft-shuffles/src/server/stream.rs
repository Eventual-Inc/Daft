use std::io::ErrorKind;

use arrow_flight::FlightData;
use common_error::{DaftError, DaftResult};
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Reading state maintenance
struct ReadState<R> {
    // The reader to read from
    reader: R,
    // The buffer to store the data
    data_buffer: Vec<u8>,
    // The buffer to store the message
    message_buffer: Vec<u8>,
}

/// State machine for stream processing
enum StreamState<R> {
    // A tuple of the current read state and the flight data to be sent
    Ready((ReadState<R>, FlightData)),
    // The read state is not done, continue reading
    Continue(ReadState<R>),
    // The read state is done, no more data to read
    Done,
}

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// A reader that reads arrow ipc files in stream format and converts them to FlightData for serving
/// over flight. This is an optimization where we skip converting the ipc files to RecordBatches
/// and instead read the data directly into FlightData, since we already know that the data is in
/// arrow ipc stream format.
pub struct FlightDataStreamReader<R: AsyncRead + Unpin> {
    state: Option<ReadState<R>>,
}

impl<R: AsyncRead + Unpin> FlightDataStreamReader<R> {
    pub async fn try_new(mut reader: R) -> DaftResult<Self> {
        // Skip stream metadata in the file since we don't need it when sending data over flight
        skip_stream_metadata(&mut reader).await?;
        Ok(Self {
            state: Some(ReadState {
                reader,
                data_buffer: Vec::new(),
                message_buffer: Vec::new(),
            }),
        })
    }

    pub fn into_stream(self) -> impl Stream<Item = DaftResult<FlightData>> {
        futures::stream::unfold(self.state, |state| async move {
            let mut current = state?;
            loop {
                match process_next(current).await {
                    Ok(StreamState::Ready((next_state, data))) => {
                        return Some((Ok(data), Some(next_state)));
                    }
                    Ok(StreamState::Continue(next_state)) => {
                        current = next_state;
                    }
                    Ok(StreamState::Done) => return None,
                    Err(e) => return Some((Err(e), None)),
                }
            }
        })
    }
}

/// Skip stream metadata on reader. We don't need it when sending data over flight.
pub async fn skip_stream_metadata<R: AsyncRead + Unpin>(reader: &mut R) -> DaftResult<()> {
    let mut meta_buf = [0u8; 4];
    AsyncReadExt::read_exact(reader, &mut meta_buf).await?;

    if meta_buf == CONTINUATION_MARKER {
        AsyncReadExt::read_exact(reader, &mut meta_buf).await?;
    }
    let meta_len = i32::from_le_bytes(meta_buf);

    let meta_len = meta_len
        .try_into()
        .map_err(|_| arrow_schema::ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    let mut meta_buffer = vec![0u8; meta_len];
    AsyncReadExt::read_exact(reader, &mut meta_buffer).await?;

    Ok(())
}

/// Process next IPC message into FlightData
async fn process_next<R: AsyncRead + Unpin>(mut state: ReadState<R>) -> DaftResult<StreamState<R>> {
    let mut meta_buf = [0u8; 4];

    match AsyncReadExt::read_exact(&mut state.reader, &mut meta_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
            return Ok(StreamState::Continue(state));
        }
        Err(e) => return Err(DaftError::from(e)),
    }

    if meta_buf == CONTINUATION_MARKER {
        AsyncReadExt::read_exact(&mut state.reader, &mut meta_buf).await?;
    }
    let meta_length = i32::from_le_bytes(meta_buf);

    let meta_length: usize = meta_length
        .try_into()
        .map_err(|_| arrow_schema::ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    if meta_length == 0 {
        return Ok(StreamState::Done);
    }

    // Read message header
    state.message_buffer.resize(meta_length, 0);
    AsyncReadExt::read_exact(&mut state.reader, &mut state.message_buffer).await?;

    // Read message body length
    let message = arrow_ipc::root_as_message(&state.message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;

    // Read message body
    state.data_buffer.resize(body_length, 0);
    AsyncReadExt::read_exact(&mut state.reader, &mut state.data_buffer).await?;

    let message_buffer = std::mem::take(&mut state.message_buffer);
    let data_buffer = std::mem::take(&mut state.data_buffer);

    let flight_data = FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    };

    Ok(StreamState::Ready((state, flight_data)))
}
