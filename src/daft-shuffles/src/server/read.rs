use std::io::Read;

use arrow2::io::ipc::read::OutOfSpecKind;
use arrow_flight::FlightData;
use common_error::{DaftError, DaftResult};

/// Reading state maintenance
struct ReadState<R> {
    reader: R,
    data_buffer: Vec<u8>,
    message_buffer: Vec<u8>,
}

/// State machine for stream processing
enum StreamState<R> {
    Ready((ReadState<R>, FlightData)),
    Continue(ReadState<R>),
    Done,
}

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Skip stream metadata on reader
pub fn skip_stream_metadata<R: Read>(reader: &mut R) -> DaftResult<()> {
    let mut meta_size = [0u8; 4];
    reader.read_exact(&mut meta_size)?;

    let meta_len = if meta_size == CONTINUATION_MARKER {
        reader.read_exact(&mut meta_size)?;
        i32::from_le_bytes(meta_size)
    } else {
        i32::from_le_bytes(meta_size)
    };

    let meta_len = meta_len
        .try_into()
        .map_err(|_| arrow2::error::Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let mut meta_buffer = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buffer)?;

    Ok(())
}

/// Process next IPC message into EncodedData
fn process_next<R: Read>(mut state: ReadState<R>) -> DaftResult<StreamState<R>> {
    let mut meta_length = [0u8; 4];

    match state.reader.read_exact(&mut meta_length) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(StreamState::Continue(state))
        }
        Err(e) => return Err(DaftError::from(e)),
    }

    let meta_length = if meta_length == CONTINUATION_MARKER {
        state.reader.read_exact(&mut meta_length)?;
        i32::from_le_bytes(meta_length)
    } else {
        i32::from_le_bytes(meta_length)
    };

    let meta_length: usize = meta_length
        .try_into()
        .map_err(|_| arrow2::error::Error::from(OutOfSpecKind::NegativeFooterLength))?;

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

    // Construct FlightData
    let flight_data = FlightData {
        data_header: state.message_buffer.clone().into(),
        data_body: state.data_buffer.clone().into(),
        ..Default::default()
    };

    Ok(StreamState::Ready((state, flight_data)))
}

pub struct FlightDataReader<R: Read> {
    state: Option<ReadState<R>>,
}

impl<R: Read> FlightDataReader<R> {
    /// Create new reader starting after schema
    pub fn try_new(mut reader: R) -> DaftResult<Self> {
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

impl<R: Read> Iterator for FlightDataReader<R> {
    type Item = DaftResult<FlightData>;

    fn next(&mut self) -> Option<Self::Item> {
        let state = match self.state.take() {
            Some(s) => s,
            None => return None,
        };

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
