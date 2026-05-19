//! Streaming helpers for Flight responses.
//!
//! - `FlightDataStreamReader` (async): reads an IPC file's bytes and yields `FlightData`
//!   without decoding to `RecordBatch`. Used by the in-process local path.
//! - `concat_specs_into_flight_data_stream` (sync inside `async_stream!`): decodes shuffle
//!   files into arrow batches, coalesces small batches up to a byte target, and IPC-encodes
//!   each chunk as `FlightData`. Used by the gRPC `do_get` path. The schema is sent
//!   separately at the head of the response, so this emits body-only `FlightData` via
//!   `IpcDataGenerator`.

use std::{
    fs::File,
    io::{BufReader, Cursor, ErrorKind, Read, Seek, SeekFrom},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_flight::FlightData;
use arrow_ipc::{
    reader::StreamReader,
    root_as_message,
    writer::{
        CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
    },
};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use common_error::{DaftError, DaftResult};
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tonic::Status;

// ---------------------------------------------------------------------------
// Async byte-to-FlightData reader (local path)
// ---------------------------------------------------------------------------

struct ReadState<R> {
    reader: R,
}

enum StreamState<R> {
    Ready((ReadState<R>, FlightData)),
    Done,
}

const CONTINUATION_MARKER: i32 = -1;

/// Reads an arrow IPC stream file and yields `FlightData` directly. Skips the
/// RecordBatch decode step since the bytes are already in IPC stream format.
pub struct FlightDataStreamReader<R: AsyncRead + Unpin> {
    state: Option<ReadState<R>>,
}

impl<R: AsyncRead + AsyncSeek + Unpin> FlightDataStreamReader<R> {
    pub async fn try_new(mut reader: R) -> DaftResult<Self> {
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

async fn process_next<R: AsyncRead + Unpin>(mut state: ReadState<R>) -> DaftResult<StreamState<R>> {
    let mut meta_len = match state.reader.read_i32_le().await {
        Ok(meta_len) => meta_len,
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
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

    let mut message_buffer = vec![0; meta_len];
    state.reader.read_exact(&mut message_buffer).await?;

    let message = root_as_message(&message_buffer)
        .map_err(|e| DaftError::InternalError(format!("Invalid flatbuffer message: {e}")))?;

    let body_length: usize = message
        .bodyLength()
        .try_into()
        .map_err(|_| DaftError::InternalError("Unexpected negative integer".to_string()))?;

    let mut data_buffer = vec![0; body_length];
    state.reader.read_exact(&mut data_buffer).await?;

    let flight_data = FlightData {
        data_header: message_buffer.into(),
        data_body: data_buffer.into(),
        ..Default::default()
    };

    Ok(StreamState::Ready((state, flight_data)))
}

// ---------------------------------------------------------------------------
// Coalescing spec-to-FlightData stream (gRPC do_get path)
// ---------------------------------------------------------------------------

/// How to read one file's contribution to a Flight response.
pub(super) enum FileReadSpec {
    /// Read the entire IPC stream file (per-partition cache).
    Whole { path: String },
    /// Read one or more `(start, end)` ranges from a single file (combined-file shuffle).
    Ranges {
        path: String,
        ranges: Vec<(u64, u64)>,
    },
}

/// IPC EOS marker: continuation marker (0xFFFFFFFF, i32 LE) + meta_len 0. Appended so
/// `StreamReader` stops at the end of our range instead of reading the next batch's bytes.
const IPC_EOS_BYTES: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/// Bytes of a schema-only IPC stream. `StreamWriter::try_new_with_options` writes the
/// schema message; dropping without `finish()` skips the EOS. Used as a prefix when
/// feeding mid-stream byte ranges into `StreamReader`.
fn build_schema_header_bytes(arrow_schema: &Schema) -> DaftResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    let _ = StreamWriter::try_new_with_options(&mut buf, arrow_schema, IpcWriteOptions::default())?;
    Ok(buf)
}

fn open_whole_file_reader(
    path: &str,
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, Status> {
    let file = File::open(path).map_err(|e| Status::internal(format!("file open: {}", e)))?;
    StreamReader::try_new(BufReader::with_capacity(64 * 1024, file), None)
        .map_err(|e| Status::internal(format!("ipc init: {}", e)))
}

/// Open a `StreamReader` over `[start, end)` of `file`. `seek+take` (not pread) keeps the
/// per-FD access pattern sequential so the kernel ramps readahead. Synthesizes a complete
/// IPC stream by prepending a schema header and appending an EOS marker.
fn open_range_reader<'a>(
    file: &'a mut File,
    start: u64,
    end: u64,
    schema_header_bytes: &[u8],
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>> + 'a, Status> {
    file.seek(SeekFrom::Start(start))
        .map_err(|e| Status::internal(format!("file seek: {}", e)))?;
    // Trait methods qualified — `take`/`chain` also exist on `tokio::io::AsyncReadExt`,
    // which is in scope for the async section above.
    let body = Read::take(&mut *file, end - start);
    let stream_bytes = Read::chain(
        Read::chain(Cursor::new(schema_header_bytes.to_vec()), body),
        Cursor::new(IPC_EOS_BYTES),
    );
    StreamReader::try_new(BufReader::with_capacity(64 * 1024, stream_bytes), None)
        .map_err(|e| Status::internal(format!("ipc init: {}", e)))
}

/// Buffers arrow batches and emits coalesced FlightData when the buffer reaches the byte
/// target. Coalescing avoids the per-message Flight tax on the client.
struct FlightDataCoalescer {
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    chunk_target_bytes: usize,
    schema: SchemaRef,
    opts: IpcWriteOptions,
    data_gen: IpcDataGenerator,
    dict_tracker: DictionaryTracker,
    compression_ctx: CompressionContext,
}

impl FlightDataCoalescer {
    fn new(schema: SchemaRef, chunk_target_bytes: usize) -> Self {
        Self {
            pending: Vec::new(),
            pending_bytes: 0,
            chunk_target_bytes,
            schema,
            opts: IpcWriteOptions::default(),
            data_gen: IpcDataGenerator::default(),
            dict_tracker: DictionaryTracker::new(false),
            compression_ctx: CompressionContext::default(),
        }
    }

    /// Buffer `batch`. If the buffer now meets the byte target, returns FlightData for
    /// the flushed group; otherwise returns an empty Vec.
    fn push(&mut self, batch: RecordBatch) -> Result<Vec<FlightData>, Status> {
        self.pending_bytes += batch.get_array_memory_size();
        self.pending.push(batch);
        if self.pending_bytes >= self.chunk_target_bytes {
            self.flush()
        } else {
            Ok(Vec::new())
        }
    }

    fn finish(mut self) -> Result<Vec<FlightData>, Status> {
        if self.pending.is_empty() {
            Ok(Vec::new())
        } else {
            self.flush()
        }
    }

    fn flush(&mut self) -> Result<Vec<FlightData>, Status> {
        let merged = concat_batches(&self.schema, self.pending.iter())
            .map_err(|e| Status::internal(format!("concat: {}", e)))?;
        let (dicts, batch) = self
            .data_gen
            .encode(
                &merged,
                &mut self.dict_tracker,
                &self.opts,
                &mut self.compression_ctx,
            )
            .map_err(|e| Status::internal(format!("encode: {}", e)))?;
        self.pending.clear();
        self.pending_bytes = 0;
        let mut out: Vec<FlightData> = dicts.into_iter().map(Into::into).collect();
        out.push(batch.into());
        Ok(out)
    }
}

/// Decode `specs` into arrow batches, coalesce them into groups ≥ `chunk_target_bytes`,
/// IPC-encode each group, and yield `FlightData` items. Uses `IpcDataGenerator` directly
/// since the schema is already sent at the start of the do_get response.
pub(super) fn concat_specs_into_flight_data_stream(
    specs: Vec<FileReadSpec>,
    arrow_schema: Arc<Schema>,
    chunk_target_bytes: usize,
) -> Result<impl Stream<Item = Result<FlightData, Status>> + Send + 'static, Status> {
    let schema_header_bytes = build_schema_header_bytes(&arrow_schema)
        .map_err(|e| Status::internal(format!("schema header: {}", e)))?;
    let mut coalescer = FlightDataCoalescer::new(arrow_schema, chunk_target_bytes);
    Ok(async_stream::try_stream! {
        for spec in specs {
            match spec {
                FileReadSpec::Whole { path } => {
                    for batch in open_whole_file_reader(&path)? {
                        let batch = batch
                            .map_err(|e| Status::internal(format!("ipc decode: {}", e)))?;
                        for d in coalescer.push(batch)? { yield d; }
                    }
                }
                FileReadSpec::Ranges { path, ranges } => {
                    let mut file = File::open(&path)
                        .map_err(|e| Status::internal(format!("file open: {}", e)))?;
                    for (start, end) in ranges {
                        for batch in open_range_reader(&mut file, start, end, &schema_header_bytes)? {
                            let batch = batch
                                .map_err(|e| Status::internal(format!("ipc decode: {}", e)))?;
                            for d in coalescer.push(batch)? { yield d; }
                        }
                    }
                }
            }
        }
        for d in coalescer.finish()? { yield d; }
    })
}
