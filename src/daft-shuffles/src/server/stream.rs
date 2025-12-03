use std::io::Read;

use arrow_flight::FlightData;
use common_error::{DaftError, DaftResult};
use daft_arrow::io::ipc::read::OutOfSpecKind;

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
pub struct FlightDataStreamReader<R: Read> {
    state: Option<ReadState<R>>,
}

impl<R: Read> FlightDataStreamReader<R> {
    pub fn try_new(mut reader: R) -> DaftResult<Self> {
        // Skip stream metadata in the file since we don't need it when sending data over flight
        skip_stream_metadata(&mut reader)?;
        Ok(Self {
            state: Some(ReadState {
                reader,
                data_buffer: Vec::new(),
                message_buffer: Vec::new(),
            }),
        })
    }
}

impl<R: Read> Iterator for FlightDataStreamReader<R> {
    type Item = DaftResult<FlightData>;

    fn next(&mut self) -> Option<Self::Item> {
        let state = self.state.take()?;

        match process_next(state) {
            Ok(StreamState::Ready((state, data))) => {
                self.state = Some(state);
                Some(Ok(data))
            }
            Ok(StreamState::Continue(state)) => {
                self.state = Some(state);
                self.next() // Recursive call to continue processing
            }
            Ok(StreamState::Done) => None,
            Err(e) => {
                self.state = None;
                Some(Err(e))
            }
        }
    }
}

/// Skip stream metadata on reader. We don't need it when sending data over flight.
pub fn skip_stream_metadata<R: Read>(reader: &mut R) -> DaftResult<()> {
    let mut meta_buf = [0u8; 4];
    reader.read_exact(&mut meta_buf)?;

    if meta_buf == CONTINUATION_MARKER {
        reader.read_exact(&mut meta_buf)?;
    }
    let meta_len = i32::from_le_bytes(meta_buf);

    let meta_len = meta_len
        .try_into()
        .map_err(|_| daft_arrow::error::Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let mut meta_buffer = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buffer)?;

    Ok(())
}

/// Process next IPC message into FlightData
fn process_next<R: Read>(mut state: ReadState<R>) -> DaftResult<StreamState<R>> {
    let mut meta_buf = [0u8; 4];

    match state.reader.read_exact(&mut meta_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(StreamState::Continue(state));
        }
        Err(e) => return Err(DaftError::from(e)),
    }

    if meta_buf == CONTINUATION_MARKER {
        state.reader.read_exact(&mut meta_buf)?;
    }
    let meta_length = i32::from_le_bytes(meta_buf);

    let meta_length: usize = meta_length
        .try_into()
        .map_err(|_| daft_arrow::error::Error::from(OutOfSpecKind::NegativeFooterLength))?;

    if meta_length == 0 {
        return Ok(StreamState::Done);
    }

    // Read message header
    state.message_buffer.resize(meta_length, 0);
    state.reader.read_exact(&mut state.message_buffer)?;

    // Read message body length
    let message = arrow_ipc::root_as_message(&state.message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;

    // Read message body
    state.data_buffer.resize(body_length, 0);
    state.reader.read_exact(&mut state.data_buffer)?;

    let message_buffer = std::mem::take(&mut state.message_buffer);
    let data_buffer = std::mem::take(&mut state.data_buffer);

    let flight_data = FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    };

    Ok(StreamState::Ready((state, flight_data)))
}
