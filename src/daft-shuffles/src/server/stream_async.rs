use std::{
    io::{Read, Seek},
    pin::Pin,
    task::{Context, Poll},
};

use arrow2::io::ipc::read::OutOfSpecKind;
use arrow_flight::FlightData;
use common_error::{DaftError, DaftResult};
use futures::{FutureExt, Stream};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
    pin,
};
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
pub struct FlightDataStreamReader<R: AsyncRead + AsyncSeek + Unpin> {
    state: Option<ReadState<R>>,
}

impl<R: AsyncRead + AsyncSeek + Unpin> FlightDataStreamReader<R> {
    pub async fn try_new(mut reader: R) -> DaftResult<Self> {
        // println!("try_new called for flight data stream reader");
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
}

impl<R: AsyncRead + AsyncSeek + Unpin> Stream for FlightDataStreamReader<R> {
    type Item = DaftResult<FlightData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // println!("poll_next called for flight data stream reader");
        let state = match self.state.take() {
            Some(state) => state,
            None => return Poll::Ready(None),
        };

        let process_next = process_next(state);
        pin!(process_next);
        loop {
            let data = (&mut process_next).poll_unpin(cx);

            match data {
                Poll::Ready(Ok(StreamState::Ready((state, data)))) => {
                    // println!("Poll::Ready(Ok(StreamState::Ready((state, data))))");
                    self.state = Some(state);
                    return Poll::Ready(Some(Ok(data)));
                }
                Poll::Ready(Ok(StreamState::Continue(state))) => {
                    // println!("Poll::Ready(Ok(StreamState::Continue(state)))");
                    self.state = Some(state);
                    return self.poll_next(cx); // Recursive call to continue processing
                }
                Poll::Pending => {
                    // println!("Poll::Pending");
                }
                Poll::Ready(Ok(StreamState::Done)) => {
                    // println!("Poll::Ready(Ok(StreamState::Done))");
                    return Poll::Ready(None);
                }
                Poll::Ready(Err(e)) => {
                    // println!("Poll::Ready(Err(e))");
                    self.state = None;
                    return Poll::Ready(Some(Err(DaftError::from(e))));
                }
            }
        }
    }
}

//     fn next(&mut self) -> Option<Self::Item> {
//         let state = self.state.take()?;

//         let data = process_next(state).await;

//         match process_next(state).await {
//             Ok(StreamState::Ready((state, data))) => {
//                 self.state = Some(state);
//                 Some(Ok(data))
//             }
//             Ok(StreamState::Continue(state)) => {
//                 self.state = Some(state);
//                 self.next() // Recursive call to continue processing
//             }
//             Ok(StreamState::Done) => None,
//             Err(e) => {
//                 self.state = None;
//                 Some(Err(e))
//             }
//         }
//     }
// }

/// Skip stream metadata on reader. We don't need it when sending data over flight.
pub async fn skip_stream_metadata<R: AsyncRead + AsyncSeek + Unpin>(
    reader: &mut R,
) -> DaftResult<()> {
    let mut meta_buf = [0u8; 4];
    reader.read_exact(&mut meta_buf).await?;

    if meta_buf == CONTINUATION_MARKER {
        reader.read_exact(&mut meta_buf).await?;
    }
    let meta_len = i32::from_le_bytes(meta_buf);

    let meta_len = meta_len
        .try_into()
        .map_err(|_| arrow2::error::Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let mut meta_buffer = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buffer).await?;

    Ok(())
}

/// Process next IPC message into FlightData
async fn process_next<R: AsyncRead + AsyncSeek + Unpin>(
    mut state: ReadState<R>,
) -> DaftResult<StreamState<R>> {
    let mut meta_buf = [0u8; 4];

    match state.reader.read_exact(&mut meta_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(StreamState::Continue(state))
        }
        Err(e) => return Err(DaftError::from(e)),
    }

    if meta_buf == CONTINUATION_MARKER {
        state.reader.read_exact(&mut meta_buf).await?;
    }
    let meta_length = i32::from_le_bytes(meta_buf);

    let meta_length: usize = meta_length
        .try_into()
        .map_err(|_| arrow2::error::Error::from(OutOfSpecKind::NegativeFooterLength))?;

    if meta_length == 0 {
        return Ok(StreamState::Done);
    }

    // Read message header
    state.message_buffer.resize(meta_length, 0);
    state.reader.read_exact(&mut state.message_buffer).await?;

    // Read message body length
    let message = arrow_ipc::root_as_message(&state.message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;

    // Read message body
    state.data_buffer.resize(body_length, 0);
    state.reader.read_exact(&mut state.data_buffer).await?;

    let message_buffer = std::mem::take(&mut state.message_buffer);
    let data_buffer = std::mem::take(&mut state.data_buffer);

    let flight_data = FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    };

    Ok(StreamState::Ready((state, flight_data)))
}
