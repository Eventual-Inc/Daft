use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::writer::IpcWriteOptions;
use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_core::{prelude::SchemaRef, series::Series};
use daft_recordbatch::RecordBatch;
use daft_schema::field::FieldRef;
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use std::{
    io::SeekFrom,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    sync::Mutex,
};
use tonic::{Request, Response, Status, transport::Server};

use super::stream::FlightDataStreamReader;
use crate::shuffle_cache::PartitionCache;

struct ParsedTicket {
    shuffle_id: u64,
    partition_ref_ids: Vec<u64>,
}

impl ParsedTicket {
    fn from_ticket(ticket: &Ticket) -> Result<Self, Status> {
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Ticket format: "shuffle_id:partition_ref_ids" where partition_ref_ids is comma-separated list of u64s
        let parts: Vec<&str> = ticket_str.splitn(2, ':').collect();
        if parts.len() < 2 {
            return Err(Status::invalid_argument(
                "Invalid ticket format. Expected 'shuffle_id:partition_ref_ids'",
            ));
        }

        let shuffle_id = parts[0]
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("Invalid shuffle id: {}", e)))?;
        let partition_ref_ids = parts[1]
            .split(',')
            .filter(|id| !id.is_empty())
            .map(|id| {
                id.parse::<u64>().map_err(|e| {
                    Status::invalid_argument(format!("Invalid partition ref id: {}", e))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            shuffle_id,
            partition_ref_ids,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FlightPartitionKey {
    shuffle_id: u64,
    partition_ref_id: u64,
}

/// How the Flight server should read one file's contribution to a partition response.
#[derive(Debug, Clone)]
enum FileReadSpec {
    /// Read the whole IPC stream file. Used by the legacy per-partition cache.
    Whole { path: String },
    /// Read [start, end) bytes from the file, treating the bytes as a stream of IPC
    /// batch messages (no schema header — the server has already emitted one).
    Range { path: String, start: u64, end: u64 },
}

/// Process-wide aggregate counters for shuffle read throughput diagnostics.
pub mod read_agg {
    use std::sync::atomic::AtomicU64;
    /// Number of `do_get` responses served by this server (remote / gRPC path).
    pub static RESPONSES: AtomicU64 = AtomicU64::new(0);
    /// Number of file specs (whole or ranged) opened across all gRPC responses.
    pub static SPECS_OPENED: AtomicU64 = AtomicU64::new(0);
    /// Total bytes of IPC body shipped to clients over gRPC.
    pub static BYTES_SHIPPED: AtomicU64 = AtomicU64::new(0);
    /// Total wall time spent inside `do_get` handler, including stream consumption.
    pub static HANDLER_US: AtomicU64 = AtomicU64::new(0);
    /// Total wall time spent opening files / seeking before streaming bytes (gRPC path).
    pub static OPEN_US: AtomicU64 = AtomicU64::new(0);

    // Local-path counters. The local path bypasses gRPC entirely; we measure it
    // separately so the local/remote split is visible in production.
    /// Number of `get_partition_local` calls served in-process.
    pub static LOCAL_RESPONSES: AtomicU64 = AtomicU64::new(0);
    /// Number of file specs (whole or ranged) opened across all local responses.
    pub static LOCAL_SPECS_OPENED: AtomicU64 = AtomicU64::new(0);
    /// Total IPC body bytes consumed by the local path (no gRPC framing involved).
    pub static LOCAL_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Total wall time spent opening files / seeking on the local path.
    pub static LOCAL_OPEN_US: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Copy)]
pub struct ReadAggSnapshot {
    pub responses: u64,
    pub specs_opened: u64,
    pub bytes_shipped: u64,
    pub handler_us: u64,
    pub open_us: u64,
    pub local_responses: u64,
    pub local_specs_opened: u64,
    pub local_bytes: u64,
    pub local_open_us: u64,
}

pub fn read_agg_snapshot() -> ReadAggSnapshot {
    ReadAggSnapshot {
        responses: read_agg::RESPONSES.load(Ordering::Relaxed),
        specs_opened: read_agg::SPECS_OPENED.load(Ordering::Relaxed),
        bytes_shipped: read_agg::BYTES_SHIPPED.load(Ordering::Relaxed),
        handler_us: read_agg::HANDLER_US.load(Ordering::Relaxed),
        open_us: read_agg::OPEN_US.load(Ordering::Relaxed),
        local_responses: read_agg::LOCAL_RESPONSES.load(Ordering::Relaxed),
        local_specs_opened: read_agg::LOCAL_SPECS_OPENED.load(Ordering::Relaxed),
        local_bytes: read_agg::LOCAL_BYTES.load(Ordering::Relaxed),
        local_open_us: read_agg::LOCAL_OPEN_US.load(Ordering::Relaxed),
    }
}

pub fn log_read_agg_summary(label: &str) {
    let s = read_agg_snapshot();
    let mb = 1024.0 * 1024.0;
    let total_bytes = s.bytes_shipped + s.local_bytes;
    let local_frac = if total_bytes == 0 {
        0.0
    } else {
        s.local_bytes as f64 / total_bytes as f64
    };
    tracing::info!(
        target: "daft_shuffles::read_agg",
        label = label,
        responses = s.responses,
        specs_opened = s.specs_opened,
        bytes_shipped_mib = format_args!("{:.1}", s.bytes_shipped as f64 / mb),
        handler_ms = s.handler_us / 1000,
        open_ms = s.open_us / 1000,
        local_responses = s.local_responses,
        local_specs_opened = s.local_specs_opened,
        local_bytes_mib = format_args!("{:.1}", s.local_bytes as f64 / mb),
        local_open_ms = s.local_open_us / 1000,
        local_frac = format_args!("{:.2}", local_frac),
        "shuffle read agg summary",
    );
}

/// How many files inside a single Flight response to open / prefetch concurrently.
/// Bench note (2026-05-11, macOS APFS, 500x500 / 1 GiB): a sweep of K=1,2,4,8,16,32 found K=1
/// (serial) is best and K>=4 actively regresses (4.0s -> 5.1s). The bottleneck on this hardware
/// is somewhere downstream of file open — likely client-side IPC decode or gRPC framing.
/// Override via DAFT_SHUFFLE_READ_PREFETCH at compile time if a production profile shows
/// open serialization is the bottleneck (e.g. on slow network FS or EBS).
const READ_PREFETCH: usize = match option_env!("DAFT_SHUFFLE_READ_PREFETCH") {
    Some(s) => parse_usize(s),
    None => 1,
};

const fn parse_usize(s: &str) -> usize {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut n: usize = 0;
    while i < bytes.len() {
        let d = (bytes[i] - b'0') as usize;
        n = n * 10 + d;
        i += 1;
    }
    n
}

#[derive(Clone, Default)]
pub struct ShuffleFlightServer {
    shuffle_partitions: Arc<Mutex<HashMap<FlightPartitionKey, PartitionCache>>>,
}

impl ShuffleFlightServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_shuffle_partitions(
        &self,
        shuffle_id: u64,
        partitions: Vec<PartitionCache>,
    ) -> DaftResult<()> {
        let mut shuffle_partitions = self.shuffle_partitions.lock().await;
        for partition in partitions {
            shuffle_partitions.insert(
                FlightPartitionKey {
                    shuffle_id,
                    partition_ref_id: partition.partition_ref_id,
                },
                partition,
            );
        }
        Ok(())
    }

    async fn get_shuffle_file_specs(
        &self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
    ) -> Option<(Vec<FileReadSpec>, SchemaRef)> {
        let partitions = self.shuffle_partitions.lock().await;

        let schema = partitions
            .get(&FlightPartitionKey {
                shuffle_id,
                partition_ref_id: *partition_ref_ids
                    .first()
                    .expect("Expected at least one partition"),
            })
            .expect("No partitions found")
            .schema
            .clone();

        let mut specs = Vec::new();
        for partition_ref_id in partition_ref_ids {
            let Some(cache) = partitions.get(&FlightPartitionKey {
                shuffle_id,
                partition_ref_id: *partition_ref_id,
            }) else {
                continue;
            };
            match &cache.byte_ranges {
                Some(ranges) => {
                    // Combined-file mode: one entry per (file_path, range).
                    for (path, (start, end)) in cache.file_paths.iter().zip(ranges.iter()) {
                        specs.push(FileReadSpec::Range {
                            path: path.clone(),
                            start: *start,
                            end: *end,
                        });
                    }
                }
                None => {
                    for path in &cache.file_paths {
                        specs.push(FileReadSpec::Whole { path: path.clone() });
                    }
                }
            }
        }

        Some((specs, schema))
    }

    /// Get partition data in-process (no gRPC). Returns a stream of Daft RecordBatches.
    /// Used when the reader runs on the same node as the shuffle server.
    ///
    /// This path bypasses the FlightData round-trip entirely: it opens IPC stream files
    /// synchronously inside `spawn_blocking` and decodes them directly to `arrow_array::RecordBatch`
    /// via `arrow_ipc::reader::StreamReader`, then wraps the columns into daft `Series`.
    /// Compared to the gRPC `do_get` path, this avoids (1) the per-batch `FlightData`
    /// alloc/wrap, (2) the per-spec synthetic schema-message construction, and
    /// (3) the `FlightRecordBatchStream` re-parse of the schema we just constructed.
    pub async fn get_partition_local(
        &self,
        shuffle_id: u64,
        partition_ref_ids: &[u64],
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let (specs, schema) = self
            .get_shuffle_file_specs(shuffle_id, partition_ref_ids)
            .await
            .ok_or_else(|| {
                DaftError::ValueError(format!(
                    "Shuffle partitions not found for shuffle {} refs {:?}",
                    shuffle_id, partition_ref_ids
                ))
            })?;

        read_agg::LOCAL_RESPONSES.fetch_add(1, Ordering::Relaxed);
        read_agg::LOCAL_SPECS_OPENED.fetch_add(specs.len() as u64, Ordering::Relaxed);

        // Build the IPC schema header once. Range specs read from a byte range that
        // does NOT include the file's schema header, so we prepend this buffer in front
        // of the range bytes. Whole specs already have the schema header at the file start
        // and do not need this prefix.
        let arrow_schema = schema.to_arrow()?;
        let schema_header_bytes = Arc::new(build_schema_header_bytes(&arrow_schema)?);
        let fields: Arc<Vec<FieldRef>> = Arc::new(
            schema
                .fields()
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect(),
        );

        let stream = futures::stream::iter(specs)
            .map(move |spec| {
                let schema = schema.clone();
                let schema_header_bytes = schema_header_bytes.clone();
                let fields = fields.clone();
                async move {
                    let batches =
                        read_spec_local(spec, schema.clone(), schema_header_bytes, fields).await?;
                    Ok::<_, DaftError>(futures::stream::iter(
                        batches.into_iter().map(Ok::<RecordBatch, DaftError>),
                    ))
                }
            })
            .buffered(READ_PREFETCH)
            .try_flatten();

        Ok(Box::pin(stream))
    }
}

/// Build the IPC bytes that represent a schema-only stream header. Produced by writing
/// an empty `StreamWriter` and dropping it without calling `finish()` (which would emit
/// the EOS marker). Used as a prefix when feeding ranged file reads into `StreamReader`.
fn build_schema_header_bytes(arrow_schema: &arrow_schema::Schema) -> DaftResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    let opts = IpcWriteOptions::default();
    let _writer = arrow_ipc::writer::StreamWriter::try_new_with_options(&mut buf, arrow_schema, opts)?;
    // Drop without finish; `try_new_with_options` already wrote the schema message,
    // and we want only those bytes — no batches, no EOS.
    Ok(buf)
}

/// IPC EOS marker: continuation marker (0xFFFFFFFF as i32 LE) + meta_len 0.
/// Appended after ranged reads so `StreamReader` stops cleanly at the end of our range
/// instead of seeing torn metadata bytes from the next batch in the underlying file.
const IPC_EOS_BYTES: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/// Open a single `FileReadSpec` and decode all batches synchronously. Runs inside
/// `spawn_blocking` so the file open + IPC decode CPU work doesn't tie up an async worker.
async fn read_spec_local(
    spec: FileReadSpec,
    schema: SchemaRef,
    schema_header_bytes: Arc<Vec<u8>>,
    fields: Arc<Vec<FieldRef>>,
) -> DaftResult<Vec<RecordBatch>> {
    get_io_runtime(true)
        .spawn_blocking(move || {
            let t0 = Instant::now();
            let result = read_spec_sync(spec, &schema_header_bytes, &fields, &schema);
            read_agg::LOCAL_OPEN_US
                .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
            result
        })
        .await?
}

fn read_spec_sync(
    spec: FileReadSpec,
    schema_header_bytes: &[u8],
    fields: &[FieldRef],
    schema: &SchemaRef,
) -> DaftResult<Vec<RecordBatch>> {
    use std::io::{BufReader as StdBufReader, Cursor, Read, Seek};

    // Build a Read source that delivers a complete IPC stream (schema + batches + EOS)
    // regardless of whether the underlying spec is Whole or Range.
    let (reader, body_bytes): (Box<dyn Read + Send>, u64) = match spec {
        FileReadSpec::Whole { path } => {
            let file = std::fs::File::open(&path)?;
            let body_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
            // 64 KiB buf; small enough to stay cache-friendly, large enough to amortize
            // syscalls on the typical 1-4 MiB per-batch flushes.
            let buf = StdBufReader::with_capacity(64 * 1024, file);
            (Box::new(buf), body_bytes)
        }
        FileReadSpec::Range { path, start, end } => {
            let mut file = std::fs::File::open(&path)?;
            file.seek(std::io::SeekFrom::Start(start))?;
            let body_bytes = end - start;
            let take = file.take(body_bytes);
            // Chain: synthetic schema header  →  batch bytes from the range  →  EOS marker.
            // Disambiguate to `std::io::Read::chain` because this file also imports
            // `tokio::io::AsyncReadExt`, which provides its own (async) `chain`.
            let schema_prefix = Cursor::new(schema_header_bytes.to_vec());
            let with_body = Read::chain(schema_prefix, take);
            let chained = Read::chain(with_body, Cursor::new(IPC_EOS_BYTES));
            (Box::new(StdBufReader::with_capacity(64 * 1024, chained)), body_bytes)
        }
    };
    read_agg::LOCAL_BYTES.fetch_add(body_bytes, Ordering::Relaxed);

    let stream_reader = arrow_ipc::reader::StreamReader::try_new(reader, None)?;
    let mut batches = Vec::new();
    for arrow_batch in stream_reader {
        let arrow_batch = arrow_batch?;
        let num_rows = arrow_batch.num_rows();
        let columns = fields
            .iter()
            .zip(arrow_batch.columns())
            .map(|(field, array)| Series::from_arrow(field.clone(), array.clone()))
            .collect::<DaftResult<Vec<_>>>()?;
        batches.push(RecordBatch::new_with_size(schema.clone(), columns, num_rows)?);
    }
    Ok(batches)
}

#[tonic::async_trait]
impl FlightService for ShuffleFlightServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!("Handshake is not supported for shuffle server")
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!("List flights is not supported for shuffle server")
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        unimplemented!("Get flight info is not supported for shuffle server")
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented!("Poll flight info is not supported for shuffle server")
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented!("Get schema is not supported for shuffle server")
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let t_handler = Instant::now();
        let ticket = request.into_inner();
        let ticket = ParsedTicket::from_ticket(&ticket)?;
        let shuffle_id = ticket.shuffle_id;
        let num_refs = ticket.partition_ref_ids.len();

        let (specs, schema) = self
            .get_shuffle_file_specs(shuffle_id, &ticket.partition_ref_ids)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Shuffle partitions not found for shuffle {} refs {:?}",
                    shuffle_id, ticket.partition_ref_ids
                ))
            })?;
        let num_specs = specs.len() as u64;
        read_agg::SPECS_OPENED.fetch_add(num_specs, Ordering::Relaxed);

        let file_stream = futures::stream::iter(specs);
        let flight_data_stream = file_stream
            .map(|spec| async move {
                let t0 = Instant::now();
                let stream = open_spec_as_flight_stream(spec)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                read_agg::OPEN_US.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
                Ok::<_, Status>(stream.map_err(|e| Status::internal(e.to_string())))
            })
            .buffered(READ_PREFETCH)
            .try_flatten();

        let options = IpcWriteOptions::default();
        let arrow_schema = schema
            .to_arrow()
            .map_err(|e| Status::internal(format!("Error converting schema to arrow: {}", e)))?;
        let flight_schema = SchemaAsIpc::new(&arrow_schema, &options).into();
        let flight_data =
            futures::stream::once(async { Ok(flight_schema) }).chain(flight_data_stream);

        // Wrap the stream to count bytes shipped as the client pulls them.
        let bytes_counter = std::sync::Arc::new(AtomicU64::new(0));
        let bytes_counter_for_stream = bytes_counter.clone();
        let counted = flight_data.inspect(move |item| {
            if let Ok(fd) = item {
                let n = fd.data_header.len() as u64 + fd.data_body.len() as u64;
                bytes_counter_for_stream.fetch_add(n, Ordering::Relaxed);
            }
        });

        // Trace the handler-level metadata (entry; we know num_specs but the actual
        // bytes/duration come from the stream below).
        tracing::debug!(
            target: "daft_shuffles::do_get",
            shuffle_id = shuffle_id,
            num_refs = num_refs,
            num_specs = num_specs,
            "do_get accepted",
        );

        // On stream end, emit aggregate counters. We can't easily attach a future
        // to the gRPC stream completion, so account for handler-entry time only
        // and accumulate bytes as the stream is consumed.
        read_agg::HANDLER_US.fetch_add(t_handler.elapsed().as_micros() as u64, Ordering::Relaxed);
        read_agg::RESPONSES.fetch_add(1, Ordering::Relaxed);
        // bytes_counter is moved into the closure; the stream itself updates BYTES_SHIPPED below.
        let bytes_counter_for_drop = bytes_counter;
        let counted = counted.chain(futures::stream::once(async move {
            let n = bytes_counter_for_drop.load(Ordering::Relaxed);
            read_agg::BYTES_SHIPPED.fetch_add(n, Ordering::Relaxed);
            // sentinel that yields nothing — we use map+filter to drop it
            Ok::<FlightData, Status>(FlightData::default())
        }))
        // strip the synthetic sentinel (data_header empty AND data_body empty)
        .filter(|item| {
            let keep = match item {
                Ok(fd) => !(fd.data_header.is_empty() && fd.data_body.is_empty()),
                Err(_) => true,
            };
            std::future::ready(keep)
        });

        Ok(Response::new(Box::pin(counted)))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!("Do put is not supported for shuffle server")
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!("Do exchange is not supported for shuffle server")
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented!("Do action is not supported for shuffle server")
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!("List actions is not supported for shuffle server")
    }
}

async fn open_spec_as_flight_stream(
    spec: FileReadSpec,
) -> DaftResult<BoxStream<'static, DaftResult<FlightData>>> {
    match spec {
        FileReadSpec::Whole { path } => {
            let file = tokio::fs::File::open(&path).await.map_err(DaftError::IoError)?;
            let reader = FlightDataStreamReader::try_new(BufReader::new(file)).await?;
            Ok(Box::pin(reader.into_stream()))
        }
        FileReadSpec::Range { path, start, end } => {
            let mut file = tokio::fs::File::open(&path).await.map_err(DaftError::IoError)?;
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(DaftError::IoError)?;
            let limited = file.take(end - start);
            let reader = FlightDataStreamReader::from_skipped(BufReader::new(limited));
            Ok(Box::pin(reader.into_stream()))
        }
    }
}

pub struct FlightServerConnectionHandle {
    ip: String,
    port: u16,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    server_task: Option<RuntimeTask<DaftResult<()>>>,
}

impl FlightServerConnectionHandle {
    pub fn shutdown(&mut self) -> DaftResult<()> {
        let Some(shutdown_signal) = self.shutdown_signal.take() else {
            return Ok(());
        };
        let _ = shutdown_signal.send(());
        let Some(server_task) = self.server_task.take() else {
            return Ok(());
        };
        common_runtime::get_io_runtime(true).block_on_current_thread(server_task)??;
        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn shuffle_address(&self) -> String {
        format!("grpc://{}:{}", self.ip, self.port)
    }
}

pub fn start_server_loop(
    ip: &str,
    server: Arc<ShuffleFlightServer>,
) -> FlightServerConnectionHandle {
    let io_runtime = common_runtime::get_io_runtime(true);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (port_tx, port_rx) = tokio::sync::oneshot::channel();

    let addr = format!("{}:0", ip);
    let server_task = io_runtime.spawn(async {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("Failed to bind to port");

        let port = listener
            .local_addr()
            .expect("Failed to get local address")
            .port();

        port_tx.send(port).expect("Failed to send port");

        let incoming = tonic::transport::server::TcpIncoming::from(listener)
            .with_nodelay(Some(true))
            .with_keepalive(None);

        let flight_server = server;
        Server::builder()
            .add_service(FlightServiceServer::from_arc(flight_server))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| DaftError::InternalError(format!("Error serving flight server: {}", e)))?;

        Ok(())
    });

    let port = port_rx.blocking_recv().expect("Failed to receive port");

    FlightServerConnectionHandle {
        ip: ip.to_string(),
        port,
        shutdown_signal: Some(shutdown_tx),
        server_task: Some(server_task),
    }
}
