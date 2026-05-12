//! Combined-file shuffle writer: one IPC file per map task, with an in-memory
//! byte-range index per output partition.
//!
//! Layout:
//!   [ IPC schema header ]               (once per file)
//!   [ batch ] [ batch ] [ batch ] ...   (interleaved across output partitions)
//!   [ EOS marker ]                      (written on close)
//!
//! For each `push_partition_data(partition_idx, mp)` call, we record the byte
//! range (start, end) the IPC StreamWriter consumed for that batch, keyed by
//! `partition_idx`. On close we emit one `PartitionCache` per output partition,
//! all pointing to the same file but with different ranges.
//!
//! Schema is shared, so the Flight server prepends a single schema message at
//! the start of a response — the ranges themselves contain only batch messages.

use std::{
    fs::File,
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use tokio::sync::Mutex;

use crate::shuffle_cache::{PartitionCache, partition_ref_id};

/// Bytes of pending data per partition before we flush it as a single IPC batch.
/// 1 MiB amortizes the ~1 KiB per-batch IPC metadata to ~0.1% overhead and keeps batches
/// under gRPC's 4 MiB default max message size with room for IPC framing. It also cuts
/// client-side per-batch decode count ~4× vs a 256 KiB threshold, which is the dominant
/// residual cost on the read side.
const COALESCE_THRESHOLD_BYTES: usize = 1024 * 1024;

/// Hard cap on the writer task's *total* per-map-task buffered bytes across all partitions.
/// When exceeded, we flush the largest-buffered partition even if it's below the coalesce
/// threshold. This is the memory-safety backstop — in normal operation the per-partition
/// threshold fires first.
///
/// Bumped to 1 GiB for the eviction experiment (log from 2026-05-12 SF1000 Q5 showed
/// 21 577 evictions vs 0 coalesce flushes — the cap was firing while the per-partition
/// threshold never did). With N=512 partitions and 1 MiB threshold, the theoretical
/// non-evicting ceiling is N * threshold = 512 MiB; doubling the cap to 1 GiB gives
/// headroom for transient overshoot without changing eviction semantics under heavier
/// skew. Per-node worst case at 16 workers: 16 GiB (~12% of r5.4xlarge RAM).
/// Override via DAFT_SHUFFLE_MAX_BUFFER_MIB at compile time.
const MAX_TOTAL_BUFFER_BYTES: usize = match option_env!("DAFT_SHUFFLE_MAX_BUFFER_MIB") {
    Some(s) => parse_usize_const(s) * 1024 * 1024,
    None => 1024 * 1024 * 1024,
};

const fn parse_usize_const(s: &str) -> usize {
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

/// A `Write` wrapper that counts bytes passed through to the inner writer.
/// Used so we can ask "what byte offset are we at?" without seeking.
struct CountingFile {
    inner: File,
    bytes_written: u64,
}

impl CountingFile {
    fn new(inner: File) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    fn position(&self) -> u64 {
        self.bytes_written
    }
}

impl Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A single batched push from a sink call: one element per output partition (already
/// ordered by partition_idx; empty MicroPartitions are skipped on the writer side).
struct WriteCommand {
    partitioned: Vec<MicroPartition>,
}

/// Process-wide aggregate counters for shuffle write throughput diagnostics.
/// Enabled at all times (atomic adds are cheap). Emitted via tracing::info on
/// `MultiPartitionShuffleCache::close()` and exposed via [`agg_snapshot`] so that
/// drivers (e.g. flotilla `RemoteFlotillaRunner`) can dump a final summary.
///
/// All times are in microseconds, all sizes in bytes. Counters are monotonic
/// across the lifetime of the process.
pub mod agg {
    use std::sync::atomic::AtomicU64;
    /// Number of `MultiPartitionShuffleCache::close()` calls completed.
    pub static CACHES_CLOSED: AtomicU64 = AtomicU64::new(0);
    /// Number of `push_partition_data` calls observed by writer tasks.
    pub static PUSHES: AtomicU64 = AtomicU64::new(0);
    /// Number of IPC RecordBatch messages emitted to disk (one per `flush_partition`).
    pub static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
    /// Subset of FLUSHES_TOTAL that fired because a partition crossed the 1 MiB coalesce threshold.
    pub static FLUSHES_COALESCE: AtomicU64 = AtomicU64::new(0);
    /// Subset of FLUSHES_TOTAL that fired under the per-task memory cap (evicting the largest).
    pub static FLUSHES_EVICTION: AtomicU64 = AtomicU64::new(0);
    /// Subset of FLUSHES_TOTAL that fired during drain on `close()`.
    pub static FLUSHES_DRAIN: AtomicU64 = AtomicU64::new(0);
    /// Total wall time spent inside `RecordBatch::concat` on the writer task.
    pub static CONCAT_US: AtomicU64 = AtomicU64::new(0);
    /// Total wall time spent inside `StreamWriter::write` (IPC encode + disk write).
    pub static ENCODE_WRITE_US: AtomicU64 = AtomicU64::new(0);
    /// Total wall time spent inside `IpcWriter::finish` (closes / flushes the file).
    pub static FINISH_US: AtomicU64 = AtomicU64::new(0);
    /// Sum of in-memory `MicroPartition::size_bytes` pushed to writer tasks (input side).
    pub static INPUT_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Sum of on-disk file sizes produced (output side, includes IPC metadata).
    pub static OUTPUT_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Number of unique files produced (i.e. number of map tasks that closed).
    pub static FILES_PRODUCED: AtomicU64 = AtomicU64::new(0);

    // Flush-size distribution buckets — count of flushes whose IPC-encoded body
    // (offset_after - offset_before, including per-batch metadata) falls in each range.
    pub static FLUSH_LT_4K: AtomicU64 = AtomicU64::new(0);
    pub static FLUSH_LT_16K: AtomicU64 = AtomicU64::new(0);
    pub static FLUSH_LT_64K: AtomicU64 = AtomicU64::new(0);
    pub static FLUSH_LT_256K: AtomicU64 = AtomicU64::new(0);
    pub static FLUSH_LT_1M: AtomicU64 = AtomicU64::new(0);
    pub static FLUSH_LT_4M: AtomicU64 = AtomicU64::new(0);
    pub static FLUSH_GE_4M: AtomicU64 = AtomicU64::new(0);
}

/// Aggregates that live on the RepartitionSink side (pre-cache). Tracks where time
/// goes between "BlockingSink::sink got called" and "push hit the writer task".
pub mod repartition_agg {
    use std::sync::atomic::AtomicU64;
    /// Number of `RepartitionSink::sink` calls dispatched.
    pub static SINK_CALLS: AtomicU64 = AtomicU64::new(0);
    /// Total wall time inside `RepartitionSink::sink` from start of the async block to
    /// state-return (includes partition_by_hash + all N pushes).
    pub static SINK_US: AtomicU64 = AtomicU64::new(0);
    /// Total rows received by sinks (sum of `MicroPartition::len()`).
    pub static SINK_INPUT_ROWS: AtomicU64 = AtomicU64::new(0);
    /// Total in-memory bytes received by sinks (sum of `MicroPartition::size_bytes()`).
    pub static SINK_INPUT_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Calls to `partition_by_hash`/`partition_by_random`/`partition_by_range` per sink call.
    pub static PARTITION_CALLS: AtomicU64 = AtomicU64::new(0);
    /// Total wall time inside the partition_by_* call.
    pub static PARTITION_US: AtomicU64 = AtomicU64::new(0);
    /// Total wall time inside the per-call `try_join_all` over `cache.push_partition_data`.
    pub static PUSH_US: AtomicU64 = AtomicU64::new(0);
    /// Number of `RepartitionSink::finalize` calls completed.
    pub static FINALIZE_CALLS: AtomicU64 = AtomicU64::new(0);
    /// Total wall time inside the cache.close() drain on finalize (excludes register).
    pub static FINALIZE_CLOSE_US: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Copy)]
pub struct RepartitionAggSnapshot {
    pub sink_calls: u64,
    pub sink_us: u64,
    pub sink_input_rows: u64,
    pub sink_input_bytes: u64,
    pub partition_calls: u64,
    pub partition_us: u64,
    pub push_us: u64,
    pub finalize_calls: u64,
    pub finalize_close_us: u64,
}

pub fn repartition_agg_snapshot() -> RepartitionAggSnapshot {
    RepartitionAggSnapshot {
        sink_calls: repartition_agg::SINK_CALLS.load(Ordering::Relaxed),
        sink_us: repartition_agg::SINK_US.load(Ordering::Relaxed),
        sink_input_rows: repartition_agg::SINK_INPUT_ROWS.load(Ordering::Relaxed),
        sink_input_bytes: repartition_agg::SINK_INPUT_BYTES.load(Ordering::Relaxed),
        partition_calls: repartition_agg::PARTITION_CALLS.load(Ordering::Relaxed),
        partition_us: repartition_agg::PARTITION_US.load(Ordering::Relaxed),
        push_us: repartition_agg::PUSH_US.load(Ordering::Relaxed),
        finalize_calls: repartition_agg::FINALIZE_CALLS.load(Ordering::Relaxed),
        finalize_close_us: repartition_agg::FINALIZE_CLOSE_US.load(Ordering::Relaxed),
    }
}

pub fn log_repartition_agg_summary(label: &str) {
    let s = repartition_agg_snapshot();
    let mb = 1024.0 * 1024.0;
    let avg_input_kib = if s.sink_calls == 0 {
        0.0
    } else {
        (s.sink_input_bytes as f64 / s.sink_calls as f64) / 1024.0
    };
    let avg_rows = if s.sink_calls == 0 {
        0
    } else {
        s.sink_input_rows / s.sink_calls
    };
    tracing::info!(
        target: "daft_shuffles::repartition_agg",
        label = label,
        sink_calls = s.sink_calls,
        sink_input_rows = s.sink_input_rows,
        sink_input_mib = format_args!("{:.1}", s.sink_input_bytes as f64 / mb),
        avg_input_kib = format_args!("{:.1}", avg_input_kib),
        avg_input_rows = avg_rows,
        sink_ms = s.sink_us / 1000,
        partition_calls = s.partition_calls,
        partition_ms = s.partition_us / 1000,
        push_ms = s.push_us / 1000,
        finalize_calls = s.finalize_calls,
        finalize_close_ms = s.finalize_close_us / 1000,
        "repartition agg summary",
    );
}

/// Legacy alias for bench compatibility; see `agg::FLUSHES_EVICTION`.
pub use agg::FLUSHES_EVICTION as CAP_EVICTIONS;

/// Snapshot all aggregate counters into a struct for one-shot logging.
#[derive(Debug, Clone, Copy)]
pub struct AggSnapshot {
    pub caches_closed: u64,
    pub pushes: u64,
    pub flushes_total: u64,
    pub flushes_coalesce: u64,
    pub flushes_eviction: u64,
    pub flushes_drain: u64,
    pub concat_us: u64,
    pub encode_write_us: u64,
    pub finish_us: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub files_produced: u64,
}

pub fn agg_snapshot() -> AggSnapshot {
    AggSnapshot {
        caches_closed: agg::CACHES_CLOSED.load(Ordering::Relaxed),
        pushes: agg::PUSHES.load(Ordering::Relaxed),
        flushes_total: agg::FLUSHES_TOTAL.load(Ordering::Relaxed),
        flushes_coalesce: agg::FLUSHES_COALESCE.load(Ordering::Relaxed),
        flushes_eviction: agg::FLUSHES_EVICTION.load(Ordering::Relaxed),
        flushes_drain: agg::FLUSHES_DRAIN.load(Ordering::Relaxed),
        concat_us: agg::CONCAT_US.load(Ordering::Relaxed),
        encode_write_us: agg::ENCODE_WRITE_US.load(Ordering::Relaxed),
        finish_us: agg::FINISH_US.load(Ordering::Relaxed),
        input_bytes: agg::INPUT_BYTES.load(Ordering::Relaxed),
        output_bytes: agg::OUTPUT_BYTES.load(Ordering::Relaxed),
        files_produced: agg::FILES_PRODUCED.load(Ordering::Relaxed),
    }
}

/// Emit a one-line aggregate-snapshot log. Call this at end of query / process for a
/// cluster-wide cost breakdown.
pub fn log_agg_summary(label: &str) {
    let s = agg_snapshot();
    let mb = 1024.0 * 1024.0;
    let lt_4k = agg::FLUSH_LT_4K.load(Ordering::Relaxed);
    let lt_16k = agg::FLUSH_LT_16K.load(Ordering::Relaxed);
    let lt_64k = agg::FLUSH_LT_64K.load(Ordering::Relaxed);
    let lt_256k = agg::FLUSH_LT_256K.load(Ordering::Relaxed);
    let lt_1m = agg::FLUSH_LT_1M.load(Ordering::Relaxed);
    let lt_4m = agg::FLUSH_LT_4M.load(Ordering::Relaxed);
    let ge_4m = agg::FLUSH_GE_4M.load(Ordering::Relaxed);
    tracing::info!(
        target: "daft_shuffles::agg",
        label = label,
        caches_closed = s.caches_closed,
        files_produced = s.files_produced,
        pushes = s.pushes,
        flushes_total = s.flushes_total,
        flushes_coalesce = s.flushes_coalesce,
        flushes_eviction = s.flushes_eviction,
        flushes_drain = s.flushes_drain,
        flush_lt_4k = lt_4k,
        flush_lt_16k = lt_16k,
        flush_lt_64k = lt_64k,
        flush_lt_256k = lt_256k,
        flush_lt_1m = lt_1m,
        flush_lt_4m = lt_4m,
        flush_ge_4m = ge_4m,
        concat_ms = s.concat_us / 1000,
        encode_write_ms = s.encode_write_us / 1000,
        finish_ms = s.finish_us / 1000,
        input_mib = format_args!("{:.1}", s.input_bytes as f64 / mb),
        output_mib = format_args!("{:.1}", s.output_bytes as f64 / mb),
        amp = format_args!(
            "{:.2}",
            if s.input_bytes == 0 { 0.0 } else { s.output_bytes as f64 / s.input_bytes as f64 }
        ),
        "shuffle agg summary",
    );
}

/// Bump the appropriate flush-size bucket. Called on every `flush_partition`.
fn record_flush_size(size_bytes: u64) {
    let bucket = if size_bytes < 4 * 1024 {
        &agg::FLUSH_LT_4K
    } else if size_bytes < 16 * 1024 {
        &agg::FLUSH_LT_16K
    } else if size_bytes < 64 * 1024 {
        &agg::FLUSH_LT_64K
    } else if size_bytes < 256 * 1024 {
        &agg::FLUSH_LT_256K
    } else if size_bytes < 1024 * 1024 {
        &agg::FLUSH_LT_1M
    } else if size_bytes < 4 * 1024 * 1024 {
        &agg::FLUSH_LT_4M
    } else {
        &agg::FLUSH_GE_4M
    };
    bucket.fetch_add(1, Ordering::Relaxed);
}

#[derive(Default)]
struct WriterTaskResult {
    /// One entry per push, keyed by partition_idx. Each push contributes one
    /// (start, end) range covering the batches written for that call.
    ranges_per_partition: Vec<Vec<(u64, u64)>>,
    rows_per_partition: Vec<usize>,
    /// Approximate in-memory bytes per partition (sum of MicroPartition::size_bytes for that idx).
    bytes_per_partition: Vec<usize>,
    file_path: String,
    total_file_bytes: u64,
}

type WriterTask = RuntimeTask<DaftResult<WriterTaskResult>>;

struct MultiCacheState {
    sender: Option<async_channel::Sender<WriteCommand>>,
    task: Option<WriterTask>,
    error: Option<String>,
}

/// Combined-file shuffle cache.
///
/// One instance per map task. Internally maintains a single async-channel feeding
/// a single writer task. Each map task therefore has 1 writer task (versus the
/// existing per-partition design of N writer tasks).
pub struct MultiPartitionShuffleCache {
    state: Mutex<MultiCacheState>,
    sender_weak: async_channel::WeakSender<WriteCommand>,
    schema: SchemaRef,
    input_id: u32,
    num_partitions: usize,
}

impl MultiPartitionShuffleCache {
    pub fn try_new(
        input_id: u32,
        num_partitions: usize,
        schema: SchemaRef,
        dirs: &[String],
        shuffle_id: u64,
    ) -> DaftResult<Self> {
        // Pick a directory by hashing on input_id (mirrors get_partition_dir in shuffle_cache.rs).
        let dir_idx = (input_id as usize) % dirs.len();
        let shuffle_dir = format!("{}/daft_shuffle/{}", dirs[dir_idx], shuffle_id);
        if !Path::new(&shuffle_dir).exists() {
            std::fs::create_dir_all(&shuffle_dir)?;
        }
        let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);

        let file = File::create(&file_path)?;
        let counting = CountingFile::new(file);

        let arrow_schema = schema.to_arrow()?;
        let write_options = arrow_ipc::writer::IpcWriteOptions::default();
        let stream_writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            counting,
            &arrow_schema,
            write_options,
        )?;

        let num_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
        let (tx, rx) = async_channel::bounded::<WriteCommand>(num_cpus * 2);
        let task_file_path = file_path.clone();
        // The writer task is end-to-end synchronous: recv from the channel, buffer,
        // IPC-encode and write to disk. Running it on `spawn_blocking` puts it on a
        // dedicated blocking thread so we don't pin an async io_runtime worker for
        // the entire lifetime of a map task, and we avoid the per-flush spawn dance.
        let task = get_io_runtime(true).spawn_blocking(move || {
            writer_task(rx, stream_writer, task_file_path, num_partitions, input_id)
        });

        let sender_weak = tx.downgrade();

        Ok(Self {
            state: Mutex::new(MultiCacheState {
                sender: Some(tx),
                task: Some(task),
                error: None,
            }),
            sender_weak,
            schema,
            input_id,
            num_partitions,
        })
    }

    /// Push one batched sink call's output (one element per partition_idx) as a single
    /// channel send. Replaces the old per-partition `push_partition_data` API; sending
    /// all N partitions in one command avoids `try_join_all` over N futures and N
    /// channel sends per sink call (the previous implementation paid 512 sends × ~6 ms
    /// of channel backpressure per sink call at SF1000 / N=512).
    pub async fn push_all(&self, partitioned: Vec<MicroPartition>) -> DaftResult<()> {
        debug_assert_eq!(
            partitioned.len(),
            self.num_partitions,
            "push_all expects one MicroPartition per output partition",
        );
        let send_result = match self.sender_weak.upgrade() {
            Some(sender) => sender
                .send(WriteCommand { partitioned })
                .await
                .map_err(|e| e.to_string()),
            None => Err("Combined shuffle cache has been closed".to_string()),
        };

        if let Err(e) = send_result {
            self.close().await?;
            return Err(DaftError::InternalError(e));
        }
        Ok(())
    }

    /// Close the writer task and emit one `PartitionCache` per output partition,
    /// each pointing to the same file with the byte ranges for its partition.
    pub async fn close(&self) -> DaftResult<Vec<PartitionCache>> {
        let mut state = self.state.lock().await;
        if let Some(err) = &state.error {
            return Err(DaftError::InternalError(err.clone()));
        }

        let sender = state.sender.take();
        let task = std::mem::take(&mut state.task);

        // Drop the sender so the writer task exits its recv loop.
        drop(sender);

        let result = match task {
            Some(t) => match t.await? {
                Ok(r) => r,
                Err(err) => {
                    state.error = Some(err.to_string());
                    return Err(err);
                }
            },
            None => {
                return Err(DaftError::InternalError(
                    "Combined shuffle cache already closed".to_string(),
                ));
            }
        };

        let WriterTaskResult {
            ranges_per_partition,
            rows_per_partition,
            bytes_per_partition,
            file_path,
            total_file_bytes,
        } = result;

        let mut caches = Vec::with_capacity(self.num_partitions);
        for partition_idx in 0..self.num_partitions {
            let ranges = ranges_per_partition
                .get(partition_idx)
                .cloned()
                .unwrap_or_default();
            let num_rows = rows_per_partition.get(partition_idx).copied().unwrap_or(0);
            let size_bytes = bytes_per_partition.get(partition_idx).copied().unwrap_or(0);

            let (file_paths, byte_ranges) = if ranges.is_empty() {
                (Vec::new(), Some(Vec::new()))
            } else {
                let paths = vec![file_path.clone(); ranges.len()];
                (paths, Some(ranges))
            };

            // bytes_per_file is informational; populate with range lengths.
            let bytes_per_file = byte_ranges
                .as_ref()
                .map(|rs| rs.iter().map(|(s, e)| (*e - *s) as usize).collect())
                .unwrap_or_default();

            caches.push(PartitionCache {
                partition_ref_id: partition_ref_id(self.input_id, partition_idx),
                schema: self.schema.clone(),
                bytes_per_file,
                file_paths,
                num_rows,
                size_bytes,
                byte_ranges,
            });
        }

        // Sanity: total_file_bytes is the on-disk size of the consolidated file. Surface it
        // for callers that want it (e.g. benches) via the file_size aggregator.
        let _ = total_file_bytes;
        Ok(caches)
    }
}

fn writer_task(
    rx: async_channel::Receiver<WriteCommand>,
    mut writer: arrow_ipc::writer::StreamWriter<CountingFile>,
    file_path: String,
    num_partitions: usize,
    input_id: u32,
) -> DaftResult<WriterTaskResult> {
    let mut ranges_per_partition: Vec<Vec<(u64, u64)>> = vec![Vec::new(); num_partitions];
    let mut rows_per_partition: Vec<usize> = vec![0; num_partitions];
    let mut bytes_per_partition: Vec<usize> = vec![0; num_partitions];

    // Per-partition buffers: pending daft RecordBatches not yet emitted as an IPC message.
    // Flushed when the partition exceeds COALESCE_THRESHOLD_BYTES or when total memory
    // pressure forces an eviction (see below).
    let mut buffers: Vec<Vec<RecordBatch>> = (0..num_partitions).map(|_| Vec::new()).collect();
    let mut buffer_bytes: Vec<usize> = vec![0; num_partitions];
    let mut total_buffer_bytes: usize = 0;

    // Per-task timing counters (microseconds).
    let mut local_concat_us: u64 = 0;
    let mut local_encode_write_us: u64 = 0;
    let mut local_pushes: u64 = 0;
    let mut local_input_bytes: u64 = 0;
    let mut local_flushes_coalesce: u64 = 0;
    let mut local_flushes_eviction: u64 = 0;
    let mut local_flushes_drain: u64 = 0;

    // Inline flush helper closure captures `&mut writer` and the timing accumulators.
    // No spawn — the writer task itself runs on spawn_blocking, so we can do CPU work
    // directly here without tying up an async worker.
    //
    // Concat is intentional: we tried writing each buffered batch as its own IPC message
    // (saving the ~50 GB memcpy of RecordBatch::concat per actor), but at SF1000 / N=512
    // that produced ~15 M tiny IPC messages, blowing on-disk amp from 1.01× to 1.34×
    // (~16 GB of extra metadata) and ballooning read-side open + decode by ~3.5×.
    // Concat costs ~50 GB memcpy = ~3 s wall on 16 cores, far cheaper than the
    // metadata + read-side blow-up.
    let mut flush_one = |partition_idx: usize,
                         batches: Vec<RecordBatch>,
                         writer: &mut arrow_ipc::writer::StreamWriter<CountingFile>,
                         concat_us: &mut u64,
                         encode_write_us: &mut u64|
     -> DaftResult<(u64, u64)> {
        let t0 = Instant::now();
        let merged = RecordBatch::concat(&batches)?;
        let arrow_batch: arrow_array::RecordBatch = merged.try_into()?;
        *concat_us += t0.elapsed().as_micros() as u64;

        let t1 = Instant::now();
        let offset_before = writer.get_ref().position();
        writer
            .write(&arrow_batch)
            .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
        let offset_after = writer.get_ref().position();
        *encode_write_us += t1.elapsed().as_micros() as u64;

        record_flush_size(offset_after - offset_before);
        ranges_per_partition[partition_idx].push((offset_before, offset_after));
        Ok((offset_before, offset_after))
    };

    while let Ok(cmd) = rx.recv_blocking() {
        let WriteCommand { partitioned } = cmd;
        debug_assert_eq!(partitioned.len(), num_partitions);

        for (partition_idx, micropartition) in partitioned.into_iter().enumerate() {
            let rows = micropartition.len();
            if rows == 0 {
                continue;
            }
            local_pushes += 1;
            for batch in micropartition.record_batches() {
                let bytes = batch.size_bytes();
                buffer_bytes[partition_idx] += bytes;
                total_buffer_bytes += bytes;
                local_input_bytes += bytes as u64;
                buffers[partition_idx].push(batch.clone());
            }
            rows_per_partition[partition_idx] += rows;
            bytes_per_partition[partition_idx] += micropartition.size_bytes();

            // Per-partition coalesce.
            if buffer_bytes[partition_idx] >= COALESCE_THRESHOLD_BYTES {
                let flushed_bytes = buffer_bytes[partition_idx];
                let batches = std::mem::take(&mut buffers[partition_idx]);
                buffer_bytes[partition_idx] = 0;
                total_buffer_bytes -= flushed_bytes;
                local_flushes_coalesce += 1;
                flush_one(
                    partition_idx,
                    batches,
                    &mut writer,
                    &mut local_concat_us,
                    &mut local_encode_write_us,
                )?;
            }
        }

        // Memory backstop: flush largest partition(s) until under cap.
        while total_buffer_bytes > MAX_TOTAL_BUFFER_BYTES {
            let Some(victim) = largest_partition(&buffer_bytes) else { break };
            let flushed_bytes = buffer_bytes[victim];
            if flushed_bytes == 0 {
                break;
            }
            let batches = std::mem::take(&mut buffers[victim]);
            buffer_bytes[victim] = 0;
            total_buffer_bytes -= flushed_bytes;
            local_flushes_eviction += 1;
            flush_one(
                victim,
                batches,
                &mut writer,
                &mut local_concat_us,
                &mut local_encode_write_us,
            )?;
        }
    }

    // Drain remaining buffers.
    for partition_idx in 0..num_partitions {
        if buffers[partition_idx].is_empty() {
            continue;
        }
        let batches = std::mem::take(&mut buffers[partition_idx]);
        local_flushes_drain += 1;
        flush_one(
            partition_idx,
            batches,
            &mut writer,
            &mut local_concat_us,
            &mut local_encode_write_us,
        )?;
    }

    let t_finish = Instant::now();
    writer
        .finish()
        .map_err(|e| DaftError::InternalError(format!("IPC finish failed: {}", e)))?;
    let finish_us = t_finish.elapsed().as_micros() as u64;
    let total_file_bytes = writer.get_ref().position();
    drop(writer);

    // Per-task summary log (cheap; only emitted at task end).
    let total_flushes = local_flushes_coalesce + local_flushes_eviction + local_flushes_drain;
    tracing::info!(
        target: "daft_shuffles::cache",
        input_id = input_id,
        num_partitions = num_partitions,
        pushes = local_pushes,
        flushes_total = total_flushes,
        flushes_coalesce = local_flushes_coalesce,
        flushes_eviction = local_flushes_eviction,
        flushes_drain = local_flushes_drain,
        concat_ms = local_concat_us / 1000,
        encode_write_ms = local_encode_write_us / 1000,
        finish_ms = finish_us / 1000,
        input_bytes = local_input_bytes,
        output_bytes = total_file_bytes,
        amp = format_args!(
            "{:.2}",
            if local_input_bytes == 0 { 0.0 } else { total_file_bytes as f64 / local_input_bytes as f64 }
        ),
        "shuffle cache closed",
    );

    // Bump aggregates.
    agg::CACHES_CLOSED.fetch_add(1, Ordering::Relaxed);
    agg::FILES_PRODUCED.fetch_add(1, Ordering::Relaxed);
    agg::PUSHES.fetch_add(local_pushes, Ordering::Relaxed);
    agg::FLUSHES_TOTAL.fetch_add(total_flushes, Ordering::Relaxed);
    agg::FLUSHES_COALESCE.fetch_add(local_flushes_coalesce, Ordering::Relaxed);
    agg::FLUSHES_EVICTION.fetch_add(local_flushes_eviction, Ordering::Relaxed);
    agg::FLUSHES_DRAIN.fetch_add(local_flushes_drain, Ordering::Relaxed);
    agg::CONCAT_US.fetch_add(local_concat_us, Ordering::Relaxed);
    agg::ENCODE_WRITE_US.fetch_add(local_encode_write_us, Ordering::Relaxed);
    agg::FINISH_US.fetch_add(finish_us, Ordering::Relaxed);
    agg::INPUT_BYTES.fetch_add(local_input_bytes, Ordering::Relaxed);
    agg::OUTPUT_BYTES.fetch_add(total_file_bytes, Ordering::Relaxed);

    Ok(WriterTaskResult {
        ranges_per_partition,
        rows_per_partition,
        bytes_per_partition,
        file_path,
        total_file_bytes,
    })
}

/// Return the index of the partition with the most pending bytes, or None if all empty.
/// O(N) per call but N is the partition count (hundreds, not millions), so this is fine.
fn largest_partition(buffer_bytes: &[usize]) -> Option<usize> {
    buffer_bytes
        .iter()
        .enumerate()
        .filter(|(_, b)| **b > 0)
        .max_by_key(|(_, b)| **b)
        .map(|(i, _)| i)
}


/// Helper for benches/tests: return the on-disk file size given the first cache (all caches share the file).
pub fn combined_file_size(caches: &[PartitionCache]) -> Option<u64> {
    let path = caches
        .iter()
        .find_map(|c| c.file_paths.first().cloned())?;
    std::fs::metadata(&path).ok().map(|m| m.len())
}

/// One-shot write of a fully-partitioned map task output to a single IPC file.
///
/// The cache-based API (`MultiPartitionShuffleCache::try_new` + `push_all` + `close`)
/// is designed for streaming: a writer task on `spawn_blocking` reads from a channel,
/// buffers per partition until a coalesce threshold or memory cap fires, and drains
/// at the end. That machinery is wasted when the caller already has the complete
/// partitioned set in hand (e.g. `RepartitionSink` accumulating raw inputs and
/// running `partition_by_hash` once at finalize). This function does exactly the
/// minimal work: open file, write schema, write each partition's batches as one
/// concat'd IPC message, record byte range, finish.
///
/// Synchronous — the caller is expected to `spawn_blocking` if running in async.
/// Bumps the same `agg` counters as the writer task, so the existing read-path
/// telemetry continues to make sense.
pub fn write_partitions_one_shot(
    input_id: u32,
    schema: SchemaRef,
    dirs: &[String],
    shuffle_id: u64,
    partitioned: Vec<MicroPartition>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitioned.len();

    // Mirror `MultiPartitionShuffleCache::try_new`'s directory + path layout so the
    // Flight server's file lookup stays consistent.
    let dir_idx = (input_id as usize) % dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", dirs[dir_idx], shuffle_id);
    if !Path::new(&shuffle_dir).exists() {
        std::fs::create_dir_all(&shuffle_dir)?;
    }
    let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);

    let file = File::create(&file_path)?;
    let counting = CountingFile::new(file);
    let arrow_schema = schema.to_arrow()?;
    let write_options = arrow_ipc::writer::IpcWriteOptions::default();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
        counting,
        &arrow_schema,
        write_options,
    )?;

    let mut ranges_per_partition: Vec<Vec<(u64, u64)>> = vec![Vec::new(); num_partitions];
    let mut rows_per_partition: Vec<usize> = vec![0; num_partitions];
    let mut bytes_per_partition: Vec<usize> = vec![0; num_partitions];

    let mut local_concat_us: u64 = 0;
    let mut local_encode_write_us: u64 = 0;
    let mut local_pushes: u64 = 0;
    let mut local_input_bytes: u64 = 0;
    let mut local_flushes: u64 = 0;

    for (partition_idx, mp) in partitioned.into_iter().enumerate() {
        let rows = mp.len();
        if rows == 0 {
            continue;
        }
        local_pushes += 1;
        let mp_bytes = mp.size_bytes();
        let batches: Vec<RecordBatch> = mp.record_batches().iter().cloned().collect();
        let batch_bytes: usize = batches.iter().map(|b| b.size_bytes()).sum();
        local_input_bytes += batch_bytes as u64;

        let t0 = Instant::now();
        let merged = RecordBatch::concat(&batches)?;
        let arrow_batch: arrow_array::RecordBatch = merged.try_into()?;
        local_concat_us += t0.elapsed().as_micros() as u64;

        let t1 = Instant::now();
        let offset_before = writer.get_ref().position();
        writer
            .write(&arrow_batch)
            .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
        let offset_after = writer.get_ref().position();
        local_encode_write_us += t1.elapsed().as_micros() as u64;

        record_flush_size(offset_after - offset_before);
        ranges_per_partition[partition_idx].push((offset_before, offset_after));
        rows_per_partition[partition_idx] = rows;
        bytes_per_partition[partition_idx] = mp_bytes;
        local_flushes += 1;
    }

    let t_finish = Instant::now();
    writer
        .finish()
        .map_err(|e| DaftError::InternalError(format!("IPC finish failed: {}", e)))?;
    let finish_us = t_finish.elapsed().as_micros() as u64;
    let total_file_bytes = writer.get_ref().position();
    drop(writer);

    tracing::info!(
        target: "daft_shuffles::cache",
        input_id = input_id,
        num_partitions = num_partitions,
        pushes = local_pushes,
        flushes_total = local_flushes,
        flushes_coalesce = 0_u64,
        flushes_eviction = 0_u64,
        flushes_drain = local_flushes,
        concat_ms = local_concat_us / 1000,
        encode_write_ms = local_encode_write_us / 1000,
        finish_ms = finish_us / 1000,
        input_bytes = local_input_bytes,
        output_bytes = total_file_bytes,
        amp = format_args!(
            "{:.2}",
            if local_input_bytes == 0 { 0.0 } else { total_file_bytes as f64 / local_input_bytes as f64 }
        ),
        "shuffle cache closed (one-shot)",
    );

    agg::CACHES_CLOSED.fetch_add(1, Ordering::Relaxed);
    agg::FILES_PRODUCED.fetch_add(1, Ordering::Relaxed);
    agg::PUSHES.fetch_add(local_pushes, Ordering::Relaxed);
    agg::FLUSHES_TOTAL.fetch_add(local_flushes, Ordering::Relaxed);
    agg::FLUSHES_DRAIN.fetch_add(local_flushes, Ordering::Relaxed);
    agg::CONCAT_US.fetch_add(local_concat_us, Ordering::Relaxed);
    agg::ENCODE_WRITE_US.fetch_add(local_encode_write_us, Ordering::Relaxed);
    agg::FINISH_US.fetch_add(finish_us, Ordering::Relaxed);
    agg::INPUT_BYTES.fetch_add(local_input_bytes, Ordering::Relaxed);
    agg::OUTPUT_BYTES.fetch_add(total_file_bytes, Ordering::Relaxed);

    let mut caches = Vec::with_capacity(num_partitions);
    for partition_idx in 0..num_partitions {
        let ranges = std::mem::take(&mut ranges_per_partition[partition_idx]);
        let num_rows = rows_per_partition[partition_idx];
        let size_bytes = bytes_per_partition[partition_idx];
        let (file_paths, byte_ranges) = if ranges.is_empty() {
            (Vec::new(), Some(Vec::new()))
        } else {
            let paths = vec![file_path.clone(); ranges.len()];
            (paths, Some(ranges))
        };
        let bytes_per_file = byte_ranges
            .as_ref()
            .map(|rs| rs.iter().map(|(s, e)| (*e - *s) as usize).collect())
            .unwrap_or_default();
        caches.push(PartitionCache {
            partition_ref_id: partition_ref_id(input_id, partition_idx),
            schema: schema.clone(),
            bytes_per_file,
            file_paths,
            num_rows,
            size_bytes,
            byte_ranges,
        });
    }
    Ok(caches)
}

