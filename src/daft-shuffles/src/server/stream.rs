//! Reading an IPC stream's bytes asynchronously and yielding `FlightData` directly,
//! skipping the `RecordBatch` decode step since the bytes are already in arrow IPC
//! stream format. Used by the local (in-process) path, the gRPC `do_get` path, and
//! the shared-mount reader.

use std::io::{ErrorKind, SeekFrom};

use arrow_flight::FlightData;
use arrow_ipc::root_as_message;
use arrow_schema::ArrowError;
use common_error::{DaftError, DaftResult};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

const CONTINUATION_MARKER: i32 = -1;

/// What [`next_flight_data`] found.
///
/// The two ways of ending are kept apart because they mean different things to
/// different callers. Reading a bounded range, the end-of-stream marker lies past
/// the range's last byte, so running out of input is the normal ending and the
/// range's length is what says whether it was complete. Reading a whole file, it
/// is the opposite: the marker is the writer's statement that it finished, and
/// running out of input instead means the file was cut short — which would
/// otherwise look exactly like a stream that simply had fewer batches in it.
pub(crate) enum FlightMessage {
    Data(FlightData),
    /// The writer's explicit end-of-stream marker.
    EndOfStream,
    /// The bytes ran out at a message boundary.
    EndOfInput,
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

/// Read the next IPC message from `reader`.
///
/// Neither ending is treated as an error here; deciding which one is acceptable
/// belongs to the caller, which is the only party that knows how much input the
/// stream was supposed to span. See [`FlightMessage`].
pub(crate) async fn next_flight_data<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> DaftResult<FlightMessage> {
    let mut meta_len = match reader.read_i32_le().await {
        Ok(meta_len) => meta_len,
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(FlightMessage::EndOfInput),
        Err(e) => return Err(DaftError::from(e)),
    };

    if meta_len == CONTINUATION_MARKER {
        meta_len = reader.read_i32_le().await?;
    }

    let meta_len: usize = meta_len
        .try_into()
        .map_err(|_| ArrowError::IpcError("NegativeFooterLength".to_string()))?;

    if meta_len == 0 {
        return Ok(FlightMessage::EndOfStream);
    }

    // Read message header
    let mut message_buffer = vec![0; meta_len];
    reader.read_exact(&mut message_buffer).await?;

    // Read message body length
    let message = root_as_message(&message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;

    // Read message body
    let mut data_buffer = vec![0; body_length];
    reader.read_exact(&mut data_buffer).await?;

    Ok(FlightMessage::Data(FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    }))
}
