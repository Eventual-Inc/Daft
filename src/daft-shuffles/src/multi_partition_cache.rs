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
};

use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use tokio::sync::Mutex;

use crate::shuffle_cache::{PartitionCache, partition_ref_id};

/// Bytes of pending data per partition before we flush it as a single IPC batch.
/// 256 KiB is big enough to amortize the ~1 KiB per-batch IPC metadata over a meaningful payload
/// (overhead < 0.5%), and small enough that we don't blow the writer task's working set.
const COALESCE_THRESHOLD_BYTES: usize = 256 * 1024;

/// Hard cap on the writer task's *total* per-map-task buffered bytes across all partitions.
/// When exceeded, we flush the largest-buffered partition even if it's below the coalesce
/// threshold. This restores bounded-memory streaming behavior — without this, a 500-partition
/// map task with sub-threshold cells could otherwise hold ~N * COALESCE_THRESHOLD ≈ 128 MiB
/// in memory until close.
const MAX_TOTAL_BUFFER_BYTES: usize = 64 * 1024 * 1024;

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

/// How many times the per-task memory-cap eviction fired during the most recent run.
/// Bench-only counter; not exposed in any production API.
pub static CAP_EVICTIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
            writer_task(rx, stream_writer, task_file_path, num_partitions).await
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

        for batch in micropartition.record_batches() {
            let bytes = batch.size_bytes();
            buffer_bytes[partition_idx] += bytes;
            total_buffer_bytes += bytes;
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
            writer = flush_partition(&io_runtime, writer, batches).await.map(
                |(w, before, after)| {
                    ranges_per_partition[partition_idx].push((before, after));
                    w
                },
            )?;
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
            CAP_EVICTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            writer = flush_partition(&io_runtime, writer, batches).await.map(
                |(w, before, after)| {
                    ranges_per_partition[victim].push((before, after));
                    w
                },
            )?;
        }
    }

    // Drain remaining buffers.
    for partition_idx in 0..num_partitions {
        if buffers[partition_idx].is_empty() {
            continue;
        }
        let batches = std::mem::take(&mut buffers[partition_idx]);
        writer = flush_partition(&io_runtime, writer, batches)
            .await
            .map(|(w, before, after)| {
                ranges_per_partition[partition_idx].push((before, after));
                w
            })?;
    }

    writer
        .finish()
        .map_err(|e| DaftError::InternalError(format!("IPC finish failed: {}", e)))?;
    let total_file_bytes = writer.get_ref().position();
    drop(writer);

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
/// and return the byte range it occupied. Done on the io runtime so concat/encode don't
/// block the writer task's recv loop.
async fn flush_partition(
    io_runtime: &common_runtime::Runtime,
    mut writer: arrow_ipc::writer::StreamWriter<CountingFile>,
    batches: Vec<RecordBatch>,
) -> DaftResult<(arrow_ipc::writer::StreamWriter<CountingFile>, u64, u64)> {
    io_runtime
        .spawn(async move {
            let merged = RecordBatch::concat(&batches)?;
            let arrow_batch: arrow_array::RecordBatch = merged.try_into()?;
            let offset_before = writer.get_ref().position();
            writer
                .write(&arrow_batch)
                .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
            let offset_after = writer.get_ref().position();
            Ok::<_, DaftError>((writer, offset_before, offset_after))
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

