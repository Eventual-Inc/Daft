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
/// threshold fires first. Sized at 256 MiB so a 16-worker node uses ~4 GiB worst case
/// (~3% of an r5.4xlarge), while still bounding pathological skew / very-high-N cases.
const MAX_TOTAL_BUFFER_BYTES: usize = 256 * 1024 * 1024;

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

struct WriteCommand {
    partition_idx: usize,
    micropartition: MicroPartition,
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
        let task = get_io_runtime(true).spawn(async move {
            writer_task(rx, stream_writer, task_file_path, num_partitions, input_id).await
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

    pub async fn push_partition_data(
        &self,
        partition_idx: usize,
        micropartition: MicroPartition,
    ) -> DaftResult<()> {
        let send_result = match self.sender_weak.upgrade() {
            Some(sender) => sender
                .send(WriteCommand {
                    partition_idx,
                    micropartition,
                })
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

async fn writer_task(
    rx: async_channel::Receiver<WriteCommand>,
    mut writer: arrow_ipc::writer::StreamWriter<CountingFile>,
    file_path: String,
    num_partitions: usize,
    input_id: u32,
) -> DaftResult<WriterTaskResult> {
    let io_runtime = get_io_runtime(true);
    let mut ranges_per_partition: Vec<Vec<(u64, u64)>> = vec![Vec::new(); num_partitions];
    let mut rows_per_partition: Vec<usize> = vec![0; num_partitions];
    let mut bytes_per_partition: Vec<usize> = vec![0; num_partitions];

    // Per-partition buffers: pending daft RecordBatches not yet emitted as an IPC message.
    // We concat them into a single arrow batch when the partition exceeds COALESCE_THRESHOLD_BYTES
    // (or when total memory pressure forces an eviction; see below).
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

    while let Ok(cmd) = rx.recv().await {
        let WriteCommand {
            partition_idx,
            micropartition,
        } = cmd;

        if partition_idx >= num_partitions {
            return Err(DaftError::InternalError(format!(
                "partition_idx {} out of bounds (num_partitions = {})",
                partition_idx, num_partitions
            )));
        }

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

        // Per-partition coalesce: emit a big batch once this partition has filled up.
        if buffer_bytes[partition_idx] >= COALESCE_THRESHOLD_BYTES {
            let flushed_bytes = buffer_bytes[partition_idx];
            let batches = std::mem::take(&mut buffers[partition_idx]);
            buffer_bytes[partition_idx] = 0;
            total_buffer_bytes -= flushed_bytes;
            local_flushes_coalesce += 1;
            let (w, before, after, c_us, ew_us) =
                flush_partition(&io_runtime, writer, batches).await?;
            ranges_per_partition[partition_idx].push((before, after));
            writer = w;
            local_concat_us += c_us;
            local_encode_write_us += ew_us;
            continue;
        }

        // Memory backstop: if total across all partitions is over budget, flush the largest
        // partition's buffer to disk regardless of size. Keeps worst-case memory bounded.
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
            let (w, before, after, c_us, ew_us) =
                flush_partition(&io_runtime, writer, batches).await?;
            ranges_per_partition[victim].push((before, after));
            writer = w;
            local_concat_us += c_us;
            local_encode_write_us += ew_us;
        }
    }

    // Drain remaining buffers.
    for partition_idx in 0..num_partitions {
        if buffers[partition_idx].is_empty() {
            continue;
        }
        let batches = std::mem::take(&mut buffers[partition_idx]);
        local_flushes_drain += 1;
        let (w, before, after, c_us, ew_us) =
            flush_partition(&io_runtime, writer, batches).await?;
        ranges_per_partition[partition_idx].push((before, after));
        writer = w;
        local_concat_us += c_us;
        local_encode_write_us += ew_us;
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

/// Concat the buffered daft batches into one arrow batch, write it as a single IPC message,
/// and return the byte range it occupied + microsecond timings for the two sub-phases
/// (concat, encode+write). Done on the io runtime so concat/encode don't block the
/// writer task's recv loop.
async fn flush_partition(
    io_runtime: &common_runtime::Runtime,
    mut writer: arrow_ipc::writer::StreamWriter<CountingFile>,
    batches: Vec<RecordBatch>,
) -> DaftResult<(arrow_ipc::writer::StreamWriter<CountingFile>, u64, u64, u64, u64)> {
    io_runtime
        .spawn(async move {
            let t0 = Instant::now();
            let merged = RecordBatch::concat(&batches)?;
            let arrow_batch: arrow_array::RecordBatch = merged.try_into()?;
            let concat_us = t0.elapsed().as_micros() as u64;

            let t1 = Instant::now();
            let offset_before = writer.get_ref().position();
            writer
                .write(&arrow_batch)
                .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
            let offset_after = writer.get_ref().position();
            let encode_write_us = t1.elapsed().as_micros() as u64;

            Ok::<_, DaftError>((
                writer,
                offset_before,
                offset_after,
                concat_us,
                encode_write_us,
            ))
        })
        .await?
}

/// Helper for benches/tests: return the on-disk file size given the first cache (all caches share the file).
pub fn combined_file_size(caches: &[PartitionCache]) -> Option<u64> {
    let path = caches
        .iter()
        .find_map(|c| c.file_paths.first().cloned())?;
    std::fs::metadata(&path).ok().map(|m| m.len())
}

