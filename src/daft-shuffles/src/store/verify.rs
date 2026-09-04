//! Reading one byte range of a shuffle file and checking, as it is consumed, that
//! what came back is what the writer put there.
//!
//! A shuffle read that quietly returns fewer rows is worse than one that fails:
//! there is no error to retry on, and the query produces a wrong answer that
//! looks like a right one. Two things can produce that, and each needs its own
//! check.
//!
//! A range can end early. The parser stops at whatever message boundary it
//! reaches, so a file truncated at a boundary — or one whose tail never reached
//! shared storage before its writer died — reads as a stream that simply had
//! fewer batches. Only the range's recorded length says otherwise.
//!
//! A range can also be the right length and the wrong bytes. A hole inside a
//! message body reads back as zeros, and an IPC message whose header is intact
//! decodes zeros into a valid-looking record batch: right row count, wrong
//! values. Only a checksum says otherwise.

use arrow_flight::FlightData;
use common_error::{DaftError, DaftResult};
use tokio::io::{AsyncRead, BufReader, ReadBuf, Take};

use crate::server::stream::{FlightMessage, next_flight_data};

/// `AsyncRead` adapter that folds every byte it hands out into a CRC-32.
struct HashingReader<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: AsyncRead + Unpin> AsyncRead for HashingReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let poll = std::pin::Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(&poll, std::task::Poll::Ready(Ok(()))) {
            this.hasher.update(&buf.filled()[before..]);
        }
        poll
    }
}

/// One byte range of a shuffle file, yielded message by message and verified at
/// the end.
///
/// The hashing sits *below* the buffering, so what is hashed is what was read
/// from the file rather than what the parser happened to ask for. That is exactly
/// the range when the parser consumed all of it — and when it did not, the
/// completeness check rejects the read before the checksum is consulted.
pub(crate) struct CheckedRange<R: AsyncRead + Unpin> {
    reader: BufReader<HashingReader<Take<R>>>,
    len: u64,
    crc32: Option<u32>,
    what: String,
}

impl<R: AsyncRead + Unpin> CheckedRange<R> {
    /// `len` is how many bytes the range should span and `crc32` their expected
    /// checksum, if the writer recorded one. `what` names the range for error
    /// messages — it is only ever read by a human diagnosing a bad shuffle.
    pub(crate) fn new(inner: R, len: u64, crc32: Option<u32>, what: String) -> Self {
        Self {
            reader: BufReader::new(HashingReader {
                inner: tokio::io::AsyncReadExt::take(inner, len),
                hasher: crc32fast::Hasher::new(),
            }),
            len,
            crc32,
            what,
        }
    }

    /// The next message, or `None` once the range is exhausted.
    ///
    /// Messages are forwarded as they are parsed, so a corrupt range can already
    /// have had earlier messages consumed downstream by the time [`Self::finish`]
    /// rejects it. That is acceptable because rejection fails the read — and so
    /// the task — rather than letting partial output stand as complete.
    pub(crate) async fn next(&mut self) -> DaftResult<Option<FlightData>> {
        match next_flight_data(&mut self.reader).await? {
            FlightMessage::Data(data) => Ok(Some(data)),
            // A range stops short of the writer's end-of-stream marker, so either
            // ending means the same thing here: no more messages. Whether that was
            // the *right* place to stop is `finish`'s question.
            FlightMessage::EndOfStream | FlightMessage::EndOfInput => Ok(None),
        }
    }

    /// Confirm the parser consumed the whole range and that its bytes match the
    /// recorded checksum. Call once [`Self::next`] has returned `None`.
    pub(crate) fn finish(self) -> DaftResult<()> {
        // Bytes the file still owes plus bytes buffered but never parsed: either
        // one means the stream ended early — a short file, or a hole read as
        // end-of-stream.
        let unconsumed = self.reader.get_ref().inner.limit() + self.reader.buffer().len() as u64;
        if unconsumed != 0 {
            return Err(DaftError::InternalError(format!(
                "{} is incomplete: {} of {} bytes unread \
                 (the writer may have died before its data reached storage)",
                self.what, unconsumed, self.len
            )));
        }
        let Some(expected) = self.crc32 else {
            return Ok(());
        };
        let actual = self.reader.get_ref().hasher.clone().finalize();
        if actual != expected {
            return Err(DaftError::InternalError(format!(
                "{} failed checksum: expected {:#010x}, got {:#010x} \
                 (the data is not what its writer committed)",
                self.what, expected, actual
            )));
        }
        Ok(())
    }
}
