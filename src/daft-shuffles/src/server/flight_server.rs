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
use crate::{
    coalescer::{self, SealConfig},
    shuffle_cache::PartitionCache,
};

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
/// One `FileReadSpec` corresponds to exactly one `open()` syscall (the first time;
/// subsequent reads hit the `OnceLock` slot lock-free). `Ranges` carries multiple
/// byte ranges within the same file (combined-file shuffle), all served from one
/// cached FD.
enum FileReadSpec {
    /// Read the whole IPC stream file. Used by the legacy per-partition cache.
    Whole {
        path: String,
        slot: Arc<std::sync::OnceLock<Arc<std::fs::File>>>,
    },
    /// Read multiple `(start, end)` byte ranges from a single file, in file order.
    /// Each range is a contiguous run of IPC batch messages.
    Ranges {
        path: String,
        slot: Arc<std::sync::OnceLock<Arc<std::fs::File>>>,
        ranges: Vec<(u64, u64)>,
    },
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

    // Fine-grained per-message breakdown (server side, gRPC path). Lets us
    // attribute total read time across disk reads vs framework overhead.
    /// Number of `FlightData` items emitted by `process_next` (1 per IPC batch).
    pub static FLIGHTDATAS_EMITTED: AtomicU64 = AtomicU64::new(0);
    /// Time inside `process_next` reading the per-message meta_len + header
    /// (4-byte length prefix + up to 64 KiB flatbuffer Message).
    pub static MSG_HEADER_READ_US: AtomicU64 = AtomicU64::new(0);
    /// Time inside `process_next` reading the message body buffer (the bulk data).
    pub static MSG_BODY_READ_US: AtomicU64 = AtomicU64::new(0);
    /// Time *between* yielding one FlightData and being polled for the next.
    /// Represents the gRPC encode/frame/network/backpressure cost the framework
    /// adds on top of our per-message work.
    pub static SEND_GAP_US: AtomicU64 = AtomicU64::new(0);

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

    // Client-side per-message breakdown. Captures whatever the framework + IPC
    // decode + cross-thread handoff costs us downstream of the server emit.
    /// Coarse: wall time between two consecutive `Poll::Ready(batch)` yields
    /// from `FlightRecordBatchStreamToDaftRecordBatchStream::poll_next`. This
    /// is the metric the original distributed profile attributed the 23 ms /
    /// 15 ms inter-poll gap to. Kept for back-compat; the four POLL_* buckets
    /// below decompose where that time actually goes.
    pub static CLIENT_BATCH_DELIVERY_US: AtomicU64 = AtomicU64::new(0);
    /// Time in the per-column arrow→daft Series conversion + RecordBatch
    /// construction, per delivered batch.
    pub static CLIENT_CONVERT_US: AtomicU64 = AtomicU64::new(0);
    /// Number of RecordBatches the client successfully decoded + converted.
    pub static CLIENT_BATCHES_RECEIVED: AtomicU64 = AtomicU64::new(0);

    // Decomposition of `CLIENT_BATCH_DELIVERY_US` into 4 buckets, by whether
    // we're measuring time inside `poll_next` (CPU we do) vs between
    // `poll_next` calls (runtime/network/consumer wait), and split by what
    // the previous poll exit returned (Ready batch → consumer drained next;
    // Pending → runtime parked waiting for IO).
    //
    // Per batch delivered, summing these buckets across the polls that
    // produced + preceded that batch reconstructs the inter-yield gap.
    /// CPU inside `poll_next` calls that returned `Poll::Ready(batch)`. This
    /// is the synchronous Flight IPC decode work that completed a batch.
    pub static CLIENT_POLL_INSIDE_READY_US: AtomicU64 = AtomicU64::new(0);
    /// CPU inside `poll_next` calls that returned `Poll::Pending`. These are
    /// polls that did partial decode work but didn't yet have enough bytes to
    /// produce a batch — the cost of state-machine churn on incomplete data.
    pub static CLIENT_POLL_INSIDE_PENDING_US: AtomicU64 = AtomicU64::new(0);
    /// Wall time between a `Pending` exit and the next `poll_next` entry —
    /// the consumer is parked, the runtime is waiting on something
    /// (gRPC network IO, decoder readiness, or a wakeup from the
    /// FlightRecordBatchStream task). Proxy for "blocked on network/IO."
    pub static CLIENT_GAP_AFTER_PENDING_US: AtomicU64 = AtomicU64::new(0);
    /// Wall time between a `Ready(batch)` yield and the next `poll_next` entry
    /// — the consumer is processing the batch we just yielded. Proxy for
    /// "blocked on downstream drain."
    pub static CLIENT_GAP_AFTER_READY_US: AtomicU64 = AtomicU64::new(0);
    /// Number of `poll_next` calls that returned `Poll::Ready(batch)`.
    /// Should equal `CLIENT_BATCHES_RECEIVED`.
    pub static CLIENT_POLL_READY_COUNT: AtomicU64 = AtomicU64::new(0);
    /// Number of `poll_next` calls that returned `Poll::Pending`.
    pub static CLIENT_POLL_PENDING_COUNT: AtomicU64 = AtomicU64::new(0);

    // Per-bucket attribution for the *read-concat* path
    // (`concat_specs_into_flight_datas_sync`). These break the current
    // catch-all `OPEN_US` (which wraps the whole spawn_blocking call) into
    // its true components.
    /// Number of `std::fs::File::open` calls in the read-concat path.
    pub static CONCAT_OPENS: AtomicU64 = AtomicU64::new(0);
    /// Time inside `File::open` syscalls (read-concat path).
    pub static CONCAT_OPEN_US: AtomicU64 = AtomicU64::new(0);
    /// Time inside `file.seek(SeekFrom::Start(start))` (read-concat path,
    /// ranged specs only).
    pub static CONCAT_SEEK_US: AtomicU64 = AtomicU64::new(0);
    /// Time spent constructing the chained reader + `StreamReader::try_new`
    /// (parses the schema header) per ranged spec.
    pub static CONCAT_STREAM_INIT_US: AtomicU64 = AtomicU64::new(0);
    /// Time inside the per-batch `arrow_ipc::reader::StreamReader::next()`
    /// loop — pulls bytes from the file + flatbuffer parse + arrow decode.
    pub static CONCAT_DECODE_US: AtomicU64 = AtomicU64::new(0);
    /// Number of source RecordBatches read from disk in the read-concat path.
    pub static CONCAT_SOURCE_BATCHES: AtomicU64 = AtomicU64::new(0);
    /// Time inside `arrow_select::concat::concat_batches` per output chunk.
    pub static CONCAT_MERGE_US: AtomicU64 = AtomicU64::new(0);
    /// Time inside `IpcDataGenerator::encode` + FlightData construction per
    /// output chunk.
    pub static CONCAT_ENCODE_US: AtomicU64 = AtomicU64::new(0);
    /// Number of output FlightData chunks emitted by the read-concat path
    /// (= number of `flush()` calls that wrote a batch).
    pub static CONCAT_OUT_CHUNKS: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Copy)]
pub struct ReadAggSnapshot {
    pub responses: u64,
    pub specs_opened: u64,
    pub bytes_shipped: u64,
    pub handler_us: u64,
    pub open_us: u64,
    pub flightdatas_emitted: u64,
    pub msg_header_read_us: u64,
    pub msg_body_read_us: u64,
    pub send_gap_us: u64,
    pub local_responses: u64,
    pub local_specs_opened: u64,
    pub local_bytes: u64,
    pub local_open_us: u64,
    pub client_batch_delivery_us: u64,
    pub client_convert_us: u64,
    pub client_batches_received: u64,
    pub client_poll_inside_ready_us: u64,
    pub client_poll_inside_pending_us: u64,
    pub client_gap_after_pending_us: u64,
    pub client_gap_after_ready_us: u64,
    pub client_poll_ready_count: u64,
    pub client_poll_pending_count: u64,
    pub concat_opens: u64,
    pub concat_open_us: u64,
    pub concat_seek_us: u64,
    pub concat_stream_init_us: u64,
    pub concat_decode_us: u64,
    pub concat_source_batches: u64,
    pub concat_merge_us: u64,
    pub concat_encode_us: u64,
    pub concat_out_chunks: u64,
}

pub fn read_agg_snapshot() -> ReadAggSnapshot {
    ReadAggSnapshot {
        responses: read_agg::RESPONSES.load(Ordering::Relaxed),
        specs_opened: read_agg::SPECS_OPENED.load(Ordering::Relaxed),
        bytes_shipped: read_agg::BYTES_SHIPPED.load(Ordering::Relaxed),
        handler_us: read_agg::HANDLER_US.load(Ordering::Relaxed),
        open_us: read_agg::OPEN_US.load(Ordering::Relaxed),
        flightdatas_emitted: read_agg::FLIGHTDATAS_EMITTED.load(Ordering::Relaxed),
        msg_header_read_us: read_agg::MSG_HEADER_READ_US.load(Ordering::Relaxed),
        msg_body_read_us: read_agg::MSG_BODY_READ_US.load(Ordering::Relaxed),
        send_gap_us: read_agg::SEND_GAP_US.load(Ordering::Relaxed),
        local_responses: read_agg::LOCAL_RESPONSES.load(Ordering::Relaxed),
        local_specs_opened: read_agg::LOCAL_SPECS_OPENED.load(Ordering::Relaxed),
        local_bytes: read_agg::LOCAL_BYTES.load(Ordering::Relaxed),
        local_open_us: read_agg::LOCAL_OPEN_US.load(Ordering::Relaxed),
        client_batch_delivery_us: read_agg::CLIENT_BATCH_DELIVERY_US.load(Ordering::Relaxed),
        client_convert_us: read_agg::CLIENT_CONVERT_US.load(Ordering::Relaxed),
        client_batches_received: read_agg::CLIENT_BATCHES_RECEIVED.load(Ordering::Relaxed),
        client_poll_inside_ready_us: read_agg::CLIENT_POLL_INSIDE_READY_US.load(Ordering::Relaxed),
        client_poll_inside_pending_us: read_agg::CLIENT_POLL_INSIDE_PENDING_US
            .load(Ordering::Relaxed),
        client_gap_after_pending_us: read_agg::CLIENT_GAP_AFTER_PENDING_US.load(Ordering::Relaxed),
        client_gap_after_ready_us: read_agg::CLIENT_GAP_AFTER_READY_US.load(Ordering::Relaxed),
        client_poll_ready_count: read_agg::CLIENT_POLL_READY_COUNT.load(Ordering::Relaxed),
        client_poll_pending_count: read_agg::CLIENT_POLL_PENDING_COUNT.load(Ordering::Relaxed),
        concat_opens: read_agg::CONCAT_OPENS.load(Ordering::Relaxed),
        concat_open_us: read_agg::CONCAT_OPEN_US.load(Ordering::Relaxed),
        concat_seek_us: read_agg::CONCAT_SEEK_US.load(Ordering::Relaxed),
        concat_stream_init_us: read_agg::CONCAT_STREAM_INIT_US.load(Ordering::Relaxed),
        concat_decode_us: read_agg::CONCAT_DECODE_US.load(Ordering::Relaxed),
        concat_source_batches: read_agg::CONCAT_SOURCE_BATCHES.load(Ordering::Relaxed),
        concat_merge_us: read_agg::CONCAT_MERGE_US.load(Ordering::Relaxed),
        concat_encode_us: read_agg::CONCAT_ENCODE_US.load(Ordering::Relaxed),
        concat_out_chunks: read_agg::CONCAT_OUT_CHUNKS.load(Ordering::Relaxed),
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
    let n = s.flightdatas_emitted.max(1);
    let cn = s.client_batches_received.max(1);
    tracing::info!(
        target: "daft_shuffles::read_agg",
        label = label,
        responses = s.responses,
        specs_opened = s.specs_opened,
        bytes_shipped_mib = format_args!("{:.1}", s.bytes_shipped as f64 / mb),
        handler_ms = s.handler_us / 1000,
        open_ms = s.open_us / 1000,
        flightdatas = s.flightdatas_emitted,
        header_read_ms = s.msg_header_read_us / 1000,
        body_read_ms = s.msg_body_read_us / 1000,
        send_gap_ms = s.send_gap_us / 1000,
        per_msg_header_us = s.msg_header_read_us / n,
        per_msg_body_us = s.msg_body_read_us / n,
        per_msg_send_gap_us = s.send_gap_us / n,
        client_batches = s.client_batches_received,
        client_delivery_ms = s.client_batch_delivery_us / 1000,
        client_convert_ms = s.client_convert_us / 1000,
        per_msg_client_delivery_us = s.client_batch_delivery_us / cn,
        per_msg_client_convert_us = s.client_convert_us / cn,
        client_poll_inside_ready_ms = s.client_poll_inside_ready_us / 1000,
        client_poll_inside_pending_ms = s.client_poll_inside_pending_us / 1000,
        client_gap_after_pending_ms = s.client_gap_after_pending_us / 1000,
        client_gap_after_ready_ms = s.client_gap_after_ready_us / 1000,
        client_poll_ready_count = s.client_poll_ready_count,
        client_poll_pending_count = s.client_poll_pending_count,
        per_batch_poll_inside_ready_us = s.client_poll_inside_ready_us / cn,
        per_batch_poll_inside_pending_us = s.client_poll_inside_pending_us / cn,
        per_batch_gap_after_pending_us = s.client_gap_after_pending_us / cn,
        per_batch_gap_after_ready_us = s.client_gap_after_ready_us / cn,
        local_responses = s.local_responses,
        local_specs_opened = s.local_specs_opened,
        local_bytes_mib = format_args!("{:.1}", s.local_bytes as f64 / mb),
        local_open_ms = s.local_open_us / 1000,
        local_frac = format_args!("{:.2}", local_frac),
        concat_opens = s.concat_opens,
        concat_open_ms = s.concat_open_us / 1000,
        concat_seek_ms = s.concat_seek_us / 1000,
        concat_stream_init_ms = s.concat_stream_init_us / 1000,
        concat_decode_ms = s.concat_decode_us / 1000,
        concat_source_batches = s.concat_source_batches,
        concat_merge_ms = s.concat_merge_us / 1000,
        concat_encode_ms = s.concat_encode_us / 1000,
        concat_out_chunks = s.concat_out_chunks,
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

#[derive(Clone)]
pub struct ShuffleFlightServer {
    shuffle_partitions: Arc<Mutex<HashMap<FlightPartitionKey, PartitionCache>>>,
    /// Tunables for `seal_shuffle`. Single shared config — there is no per-shuffle
    /// state to track because seal is single-shot per shuffle.
    seal_cfg: SealConfig,
    /// Monotonic counter for naming consolidated output files during seal.
    coalesce_seq: Arc<AtomicU64>,
}

impl Default for ShuffleFlightServer {
    fn default() -> Self {
        Self::new()
    }
}

impl ShuffleFlightServer {
    pub fn new() -> Self {
        Self::with_seal_config(SealConfig::default())
    }

    pub fn with_seal_config(seal_cfg: SealConfig) -> Self {
        Self {
            shuffle_partitions: Arc::new(Mutex::new(HashMap::new())),
            seal_cfg,
            coalesce_seq: Arc::new(AtomicU64::new(1)),
        }
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

    /// Consolidate every `(shuffle_id, partition_idx)` group in the cache: rewrite
    /// the per-task entries' source bytes into a single file per partition, then
    /// update the cache entries to point at it. Called once per shuffle by the
    /// orchestrator after the producer stage has fully drained and before any
    /// consumer tasks start fetching.
    ///
    /// Every group with >1 entry gets consolidated. Idempotent: a second call
    /// finds nothing to do.
    ///
    /// Note: with read-side server-side concat (the default), this seal phase is
    /// not required for correctness — clients receive coalesced batches from the
    /// original per-task files. Kept for callers that prefer the consolidated
    /// on-disk layout (e.g. for long-lived caches).
    pub async fn seal_shuffle(&self, shuffle_id: u64) -> DaftResult<()> {
        // Snapshot every entry for this shuffle under the lock, group by
        // partition_idx (low 32 bits of partition_ref_id).
        let by_partition: HashMap<usize, Vec<(u64, PartitionCache)>> = {
            let partitions = self.shuffle_partitions.lock().await;
            let mut by_partition: HashMap<usize, Vec<(u64, PartitionCache)>> = HashMap::new();
            for (key, cache) in partitions.iter() {
                if key.shuffle_id != shuffle_id {
                    continue;
                }
                let partition_idx = (key.partition_ref_id & 0xFFFF_FFFF) as usize;
                by_partition
                    .entry(partition_idx)
                    .or_default()
                    .push((key.partition_ref_id, cache.clone()));
            }
            by_partition
        };

        // Only consolidate groups with >1 entry *carrying data*. After
        // pipelined seal, a partition may have many empty entries plus one
        // merged entry — re-running merge against that produces the same
        // result, so skip it.
        let groups: Vec<(usize, Vec<(u64, PartitionCache)>)> = by_partition
            .into_iter()
            .filter(|(_, v)| {
                v.iter().filter(|(_, c)| !c.file_paths.is_empty()).count() > 1
            })
            .collect();
        if groups.is_empty() {
            return Ok(());
        }

        let permits = Arc::new(tokio::sync::Semaphore::new(self.seal_cfg.max_concurrent));
        let effective_cfg = self.seal_cfg;

        // Fan out plan-build + execute_plan across partitions. Each task pulls
        // a permit so total in-flight disk I/O stays bounded.
        let mut handles = Vec::with_capacity(groups.len());
        for (partition_idx, entries) in groups {
            let seq = self.coalesce_seq.fetch_add(1, Ordering::Relaxed);
            let Some(plan) = coalescer::build_plan(
                shuffle_id,
                partition_idx,
                entries.iter().map(|(r, c)| (*r, c)),
                seq,
            ) else {
                continue;
            };
            let permits = permits.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permits.acquire_owned().await.ok();
                coalescer::execute_plan(plan, effective_cfg).await
            }));
        }

        // Collect outcomes. If any individual coalesce fails, leave its original
        // entries in place and surface the error — the seal is best-effort per
        // group; correctness is preserved by not swapping for failed groups.
        let mut updated_entries: Vec<(u64, PartitionCache)> = Vec::new();
        let mut orphaned_sources: Vec<String> = Vec::new();
        let mut first_err: Option<DaftError> = None;
        for jh in handles {
            match jh.await {
                Ok(Ok(outcome)) => {
                    updated_entries.extend(outcome.updated_entries);
                    orphaned_sources.extend(outcome.orphaned_sources);
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        target: "daft_shuffles::seal",
                        shuffle_id = shuffle_id,
                        error = %e,
                        "seal: per-group coalesce failed"
                    );
                    first_err.get_or_insert(e);
                }
                Err(join_err) => {
                    tracing::warn!(
                        target: "daft_shuffles::seal",
                        shuffle_id = shuffle_id,
                        error = %join_err,
                        "seal: per-group coalesce task panicked"
                    );
                    first_err.get_or_insert_with(|| DaftError::InternalError(join_err.to_string()));
                }
            }
        }

        // Single atomic swap of all successful outcomes. Failed groups keep
        // their original entries — reads against them stay correct, just slower.
        {
            let mut partitions = self.shuffle_partitions.lock().await;
            for (ref_id, new_cache) in updated_entries {
                partitions.insert(
                    FlightPartitionKey {
                        shuffle_id,
                        partition_ref_id: ref_id,
                    },
                    new_cache,
                );
            }
        }
        for path in orphaned_sources {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(
                    target: "daft_shuffles::seal",
                    path = %path,
                    error = %e,
                    "failed to unlink coalesced source"
                );
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
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

        // Group ranged reads by file path so each physical file is read from a single
        // FD across this response (and across other responses too — the `Arc<OnceLock>`
        // slot is shared via the PartitionCache).
        let mut whole_specs = Vec::new();
        let mut ranges_by_path: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
        let mut slot_by_path: HashMap<String, Arc<std::sync::OnceLock<Arc<std::fs::File>>>> =
            HashMap::new();
        let mut first_seen: HashMap<String, usize> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for partition_ref_id in partition_ref_ids {
            let Some(cache) = partitions.get(&FlightPartitionKey {
                shuffle_id,
                partition_ref_id: *partition_ref_id,
            }) else {
                continue;
            };
            match &cache.byte_ranges {
                Some(ranges) => {
                    for ((path, slot), (start, end)) in cache
                        .file_paths
                        .iter()
                        .zip(cache.file_slots.iter())
                        .zip(ranges.iter())
                    {
                        if !first_seen.contains_key(path) {
                            first_seen.insert(path.clone(), order.len());
                            order.push(path.clone());
                            slot_by_path.insert(path.clone(), slot.clone());
                        }
                        ranges_by_path
                            .entry(path.clone())
                            .or_default()
                            .push((*start, *end));
                    }
                }
                None => {
                    for (path, slot) in
                        cache.file_paths.iter().zip(cache.file_slots.iter())
                    {
                        whole_specs.push(FileReadSpec::Whole {
                            path: path.clone(),
                            slot: slot.clone(),
                        });
                    }
                }
            }
        }

        let mut specs = whole_specs;
        for path in order {
            let mut ranges = ranges_by_path.remove(&path).unwrap_or_default();
            // Sort by start so sequential reads are forward seeks (kind to readahead).
            ranges.sort_unstable_by_key(|r| r.0);
            let slot = slot_by_path.remove(&path).expect("slot_by_path must contain path");
            specs.push(FileReadSpec::Ranges { path, slot, ranges });
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

/// gRPC `do_action` type string for orchestrator-driven seal-time consolidation.
/// The action body is a little-endian `u64` shuffle_id.
pub const SEAL_SHUFFLE_ACTION: &str = "seal_shuffle";

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

    use std::os::unix::fs::FileExt;

    let mut batches = Vec::new();
    match spec {
        FileReadSpec::Whole { path, slot } => {
            let file = crate::shuffle_cache::slot_or_open(&slot, &path)?;
            let body_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
            read_agg::LOCAL_BYTES.fetch_add(body_bytes, Ordering::Relaxed);
            let reader = StdBufReader::with_capacity(64 * 1024, (&*file).try_clone()?);
            let stream_reader = arrow_ipc::reader::StreamReader::try_new(reader, None)?;
            decode_batches_into(stream_reader, fields, schema, &mut batches)?;
        }
        FileReadSpec::Ranges { path, slot, ranges } => {
            let file = crate::shuffle_cache::slot_or_open(&slot, &path)?;
            let mut body_bytes_acc: u64 = 0;
            for (start, end) in ranges {
                let body_bytes = end - start;
                body_bytes_acc += body_bytes;
                let mut body = vec![0u8; body_bytes as usize];
                file.read_exact_at(&mut body, start)?;
                let schema_prefix = Cursor::new(schema_header_bytes.to_vec());
                let with_body = Read::chain(schema_prefix, Cursor::new(body));
                let chained = Read::chain(with_body, Cursor::new(IPC_EOS_BYTES));
                let reader = StdBufReader::with_capacity(64 * 1024, chained);
                let stream_reader = arrow_ipc::reader::StreamReader::try_new(reader, None)?;
                decode_batches_into(stream_reader, fields, schema, &mut batches)?;
            }
            read_agg::LOCAL_BYTES.fetch_add(body_bytes_acc, Ordering::Relaxed);
        }
    }
    Ok(batches)
}

fn decode_batches_into<R: std::io::Read>(
    stream_reader: arrow_ipc::reader::StreamReader<R>,
    fields: &[FieldRef],
    schema: &SchemaRef,
    out: &mut Vec<RecordBatch>,
) -> DaftResult<()> {
    for arrow_batch in stream_reader {
        let arrow_batch = arrow_batch?;
        let num_rows = arrow_batch.num_rows();
        let columns = fields
            .iter()
            .zip(arrow_batch.columns())
            .map(|(field, array)| Series::from_arrow(field.clone(), array.clone()))
            .collect::<DaftResult<Vec<_>>>()?;
        out.push(RecordBatch::new_with_size(schema.clone(), columns, num_rows)?);
    }
    Ok(())
}

/// Decode every source batch from `specs` directly into arrow `RecordBatch` (no
/// daft conversion), concat in groups whose accumulated byte size crosses
/// `chunk_target_bytes`, IPC-encode each group, and return the resulting
/// `FlightData` items. Runs synchronously inside `spawn_blocking`.
///
/// This is the read-side server-side concat path: instead of streaming M tiny
/// raw FlightDatas per partition (each paying the ~190 μs Flight per-message
/// tax on the client), we collapse the M batches into one or a few big ones
/// before the wire. Memory transient = ~chunk_target_bytes per concurrent
/// `do_get` (one builder accumulator at a time).
fn concat_specs_into_flight_datas_sync(
    specs: Vec<FileReadSpec>,
    arrow_schema: &arrow_schema::Schema,
    schema_header_bytes: &[u8],
    chunk_target_bytes: usize,
) -> DaftResult<Vec<FlightData>> {
    use std::io::{BufReader as StdBufReader, Cursor, Read};
    use std::os::unix::fs::FileExt;

    let mut pending: Vec<arrow_array::RecordBatch> = Vec::new();
    let mut pending_bytes: usize = 0;
    let mut flight_datas: Vec<FlightData> = Vec::new();
    let arrow_schema_ref: arrow_schema::SchemaRef = std::sync::Arc::new(arrow_schema.clone());
    let opts = IpcWriteOptions::default();
    // The data generator is the lowest-level encoder; `batches_to_flight_data`
    // would prepend a schema FlightData per call, which we don't want — schema
    // is already sent once at the start of the do_get response.
    let data_gen = arrow_ipc::writer::IpcDataGenerator::default();
    let mut dict_tracker = arrow_ipc::writer::DictionaryTracker::new(false);
    let mut compression_ctx = arrow_ipc::writer::CompressionContext::default();

    let mut flush = |pending: &mut Vec<arrow_array::RecordBatch>,
                     pending_bytes: &mut usize,
                     flight_datas: &mut Vec<FlightData>,
                     dict_tracker: &mut arrow_ipc::writer::DictionaryTracker,
                     compression_ctx: &mut arrow_ipc::writer::CompressionContext|
     -> DaftResult<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let t_m = Instant::now();
        let merged = arrow_select::concat::concat_batches(&arrow_schema_ref, pending.iter())
            .map_err(|e| DaftError::InternalError(format!("read-side concat: {}", e)))?;
        read_agg::CONCAT_MERGE_US
            .fetch_add(t_m.elapsed().as_micros() as u64, Ordering::Relaxed);
        let t_e = Instant::now();
        let (dicts, batch) = data_gen
            .encode(&merged, dict_tracker, &opts, compression_ctx)
            .map_err(|e| DaftError::InternalError(format!("read-side encode: {}", e)))?;
        for d in dicts {
            flight_datas.push(d.into());
        }
        flight_datas.push(batch.into());
        read_agg::CONCAT_ENCODE_US
            .fetch_add(t_e.elapsed().as_micros() as u64, Ordering::Relaxed);
        read_agg::CONCAT_OUT_CHUNKS.fetch_add(1, Ordering::Relaxed);
        pending.clear();
        *pending_bytes = 0;
        Ok(())
    };

    for spec in specs {
        match spec {
            FileReadSpec::Whole { path, slot } => {
                let t_o = Instant::now();
                let file = crate::shuffle_cache::slot_or_open(&slot, &path)?;
                read_agg::CONCAT_OPEN_US
                    .fetch_add(t_o.elapsed().as_micros() as u64, Ordering::Relaxed);
                read_agg::CONCAT_OPENS.fetch_add(1, Ordering::Relaxed);
                let body_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
                read_agg::LOCAL_BYTES.fetch_add(body_bytes, Ordering::Relaxed);
                let t_si = Instant::now();
                // Whole-file reads use Read+Seek; dup the FD so concurrent whole-file
                // readers don't share seek state. Only the legacy multi_file path
                // produces Whole specs; on the hot oneshot path this branch is unused.
                let owned = file.try_clone()?;
                let reader = StdBufReader::with_capacity(64 * 1024, owned);
                let stream_reader = arrow_ipc::reader::StreamReader::try_new(reader, None)?;
                read_agg::CONCAT_STREAM_INIT_US
                    .fetch_add(t_si.elapsed().as_micros() as u64, Ordering::Relaxed);
                let mut stream_reader = stream_reader;
                loop {
                    let t_d = Instant::now();
                    let next = stream_reader.next();
                    read_agg::CONCAT_DECODE_US
                        .fetch_add(t_d.elapsed().as_micros() as u64, Ordering::Relaxed);
                    match next {
                        Some(b) => {
                            let batch = b?;
                            read_agg::CONCAT_SOURCE_BATCHES.fetch_add(1, Ordering::Relaxed);
                            pending_bytes += estimate_batch_size(&batch);
                            pending.push(batch);
                            if pending_bytes >= chunk_target_bytes {
                                flush(
                                    &mut pending,
                                    &mut pending_bytes,
                                    &mut flight_datas,
                                    &mut dict_tracker,
                                    &mut compression_ctx,
                                )?;
                            }
                        }
                        None => break,
                    }
                }
            }
            FileReadSpec::Ranges { path, slot, ranges } => {
                let t_o = Instant::now();
                let file = crate::shuffle_cache::slot_or_open(&slot, &path)?;
                read_agg::CONCAT_OPEN_US
                    .fetch_add(t_o.elapsed().as_micros() as u64, Ordering::Relaxed);
                read_agg::CONCAT_OPENS.fetch_add(1, Ordering::Relaxed);
                let mut body_bytes_acc: u64 = 0;
                for (start, end) in ranges {
                    let body_bytes = end - start;
                    body_bytes_acc += body_bytes;
                    let mut body = vec![0u8; body_bytes as usize];
                    let t_sk = Instant::now();
                    file.read_exact_at(&mut body, start)?;
                    read_agg::CONCAT_SEEK_US
                        .fetch_add(t_sk.elapsed().as_micros() as u64, Ordering::Relaxed);
                    let schema_prefix = Cursor::new(schema_header_bytes.to_vec());
                    let with_body = Read::chain(schema_prefix, Cursor::new(body));
                    let chained = Read::chain(with_body, Cursor::new(IPC_EOS_BYTES));
                    let t_si = Instant::now();
                    let reader = StdBufReader::with_capacity(64 * 1024, chained);
                    let stream_reader =
                        arrow_ipc::reader::StreamReader::try_new(reader, None)?;
                    read_agg::CONCAT_STREAM_INIT_US
                        .fetch_add(t_si.elapsed().as_micros() as u64, Ordering::Relaxed);
                    let mut stream_reader = stream_reader;
                    loop {
                        let t_d = Instant::now();
                        let next = stream_reader.next();
                        read_agg::CONCAT_DECODE_US
                            .fetch_add(t_d.elapsed().as_micros() as u64, Ordering::Relaxed);
                        match next {
                            Some(b) => {
                                let batch = b?;
                                read_agg::CONCAT_SOURCE_BATCHES.fetch_add(1, Ordering::Relaxed);
                                pending_bytes += estimate_batch_size(&batch);
                                pending.push(batch);
                                if pending_bytes >= chunk_target_bytes {
                                    flush(
                                        &mut pending,
                                        &mut pending_bytes,
                                        &mut flight_datas,
                                        &mut dict_tracker,
                                        &mut compression_ctx,
                                    )?;
                                }
                            }
                            None => break,
                        }
                    }
                }
                read_agg::LOCAL_BYTES.fetch_add(body_bytes_acc, Ordering::Relaxed);
            }
        }
    }
    flush(
        &mut pending,
        &mut pending_bytes,
        &mut flight_datas,
        &mut dict_tracker,
        &mut compression_ctx,
    )?;
    Ok(flight_datas)
}

/// Cheap per-batch size estimate: sum of buffer lengths across columns. Used
/// to gate the concat chunker so we don't depend on parsing arrow internals.
fn estimate_batch_size(batch: &arrow_array::RecordBatch) -> usize {
    let mut total: usize = 0;
    for col in batch.columns() {
        let data = col.to_data();
        for buf in data.buffers() {
            total += buf.len();
        }
        for child in data.child_data() {
            for buf in child.buffers() {
                total += buf.len();
            }
        }
    }
    total
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

        let options = IpcWriteOptions::default();
        let arrow_schema = schema
            .to_arrow()
            .map_err(|e| Status::internal(format!("Error converting schema to arrow: {}", e)))?;
        let flight_schema = SchemaAsIpc::new(&arrow_schema, &options).into();

        // Read-side concat: server reads M source batches, concats arrow-level
        // into chunks of `chunk_target_bytes`, emits fewer/bigger FlightData
        // items. Eliminates the client's per-message Flight tax at the cost of
        // one in-memory concat per chunk. Shares its target with the oneshot
        // writer's emit size — by emitting at the same chunk size the read
        // side targets, the writer hands the server bytes already correctly
        // sized and read-concat needs neither split nor combine to right-size
        // a payload. Override via DAFT_SHUFFLE_CHUNK_BYTES; set to 0 to
        // disable read-concat (the writer's emit chunking is unaffected).
        let read_concat: Option<usize> = crate::shuffle_cache::read_chunk_target_bytes();

        let flight_data_stream: Pin<
            Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>,
        > = if let Some(chunk_target) = read_concat {
            let arrow_schema_owned = arrow_schema.clone();
            let header_bytes = build_schema_header_bytes(&arrow_schema_owned)
                .map_err(|e| Status::internal(format!("schema header probe: {}", e)))?;
            let t0 = Instant::now();
            let flight_datas = get_io_runtime(true)
                .spawn_blocking(move || {
                    concat_specs_into_flight_datas_sync(
                        specs,
                        &arrow_schema_owned,
                        &header_bytes,
                        chunk_target,
                    )
                })
                .await
                .map_err(|e| Status::internal(format!("read-concat join: {}", e)))?
                .map_err(|e| Status::internal(format!("read-concat decode: {}", e)))?;
            read_agg::OPEN_US.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
            let n = flight_datas.len() as u64;
            read_agg::FLIGHTDATAS_EMITTED.fetch_add(n, Ordering::Relaxed);
            Box::pin(futures::stream::iter(flight_datas.into_iter().map(Ok)))
        } else {
            let file_stream = futures::stream::iter(specs);
            Box::pin(
                file_stream
                    .map(|spec| async move {
                        let t0 = Instant::now();
                        let stream = open_spec_as_flight_stream(spec)
                            .await
                            .map_err(|e| Status::internal(e.to_string()))?;
                        read_agg::OPEN_US
                            .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
                        Ok::<_, Status>(stream.map_err(|e| Status::internal(e.to_string())))
                    })
                    .buffered(READ_PREFETCH)
                    .try_flatten(),
            )
        };

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
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            // Seal-time consolidation. Body is u64 shuffle_id (little-endian, 8 bytes).
            // Called once per shuffle by the orchestrator after the producer stage
            // drains; consolidates every (shuffle_id, partition_idx) group into a
            // single file. Returns an empty result on success.
            SEAL_SHUFFLE_ACTION => {
                if action.body.len() != 8 {
                    return Err(Status::invalid_argument(format!(
                        "seal_shuffle expects 8-byte shuffle_id body, got {} bytes",
                        action.body.len()
                    )));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&action.body);
                let shuffle_id = u64::from_le_bytes(buf);
                let t0 = Instant::now();
                self.seal_shuffle(shuffle_id)
                    .await
                    .map_err(|e| Status::internal(format!("seal_shuffle failed: {}", e)))?;
                tracing::info!(
                    target: "daft_shuffles::seal",
                    shuffle_id = shuffle_id,
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "seal_shuffle completed"
                );
                let result = arrow_flight::Result { body: Default::default() };
                let stream = futures::stream::once(async move { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            other => Err(Status::invalid_argument(format!(
                "unsupported action type: {}",
                other
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: SEAL_SHUFFLE_ACTION.to_string(),
            description: "Consolidate all (shuffle_id, partition_idx) groups for the given shuffle".to_string(),
        }];
        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}

async fn open_spec_as_flight_stream(
    spec: FileReadSpec,
) -> DaftResult<BoxStream<'static, DaftResult<FlightData>>> {
    match spec {
        FileReadSpec::Whole { path, slot: _ } => {
            // Raw gRPC path (no read-concat): the OnceLock cache is bypassed
            // here in favour of tokio::fs's async open; this branch is rarely
            // exercised in production where read-concat is enabled by default.
            let file = tokio::fs::File::open(&path).await.map_err(DaftError::IoError)?;
            let reader = FlightDataStreamReader::try_new(BufReader::new(file)).await?;
            Ok(Box::pin(reader.into_stream()))
        }
        FileReadSpec::Ranges { path, slot: _, ranges } => {
            // One file open serves all ranges. We hold ownership of `file` across the
            // outer for-loop; each iteration borrows it mutably via `(&mut file).take(n)`
            // for the duration of one range's stream. The borrow is released when the
            // inner stream is dropped at end of the iteration body, allowing the next
            // seek + take.
            let file = tokio::fs::File::open(&path).await.map_err(DaftError::IoError)?;
            let stream = async_stream::try_stream! {
                let mut file = file;
                for (start, end) in ranges {
                    file.seek(SeekFrom::Start(start)).await.map_err(DaftError::IoError)?;
                    let limited = (&mut file).take(end - start);
                    let reader = FlightDataStreamReader::from_skipped(BufReader::new(limited));
                    // The unfold-backed stream is !Unpin; pin it on the heap so we can
                    // call .next().await on it.
                    let mut inner = Box::pin(reader.into_stream());
                    while let Some(item) = inner.next().await {
                        yield item?;
                    }
                }
            };
            Ok(Box::pin(stream))
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
        // HTTP/2 transport tuning: matches the client side
        // (`crate::client::FlightClientManager::get_or_connect`). Larger
        // negotiated frame size + window sizes collapse the per-batch
        // wakeup count on the receiver — see comment on
        // `shuffle_cache::DEFAULT_HTTP2_*` for the diagnostic that motivated
        // these.
        Server::builder()
            .max_frame_size(Some(crate::shuffle_cache::http2_max_frame_size()))
            .initial_stream_window_size(Some(crate::shuffle_cache::http2_stream_window()))
            .initial_connection_window_size(Some(crate::shuffle_cache::http2_conn_window()))
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
