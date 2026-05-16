//! One-shot combined-file shuffle writer.
//!
//! Used by `RepartitionSink` on the Flight backend. Callers accumulate
//! per-output-partition `Vec<MicroPartition>` in memory during `sink` (same as
//! ray-plasma), and at finalize hand the whole accumulator to
//! `write_partitions_one_shot`. This does one `MicroPartition::concat`, one
//! IPC encode, and one disk write per output partition — vs the streaming
//! writer which paid a concat-encode-write cycle every time a partition's
//! buffer crossed `COALESCE_THRESHOLD_BYTES`.
//!
//! On-disk layout:
//!   [ IPC schema header ]
//!   [ partition 0 batch ] [ partition 1 batch ] ... [ partition N-1 batch ]
//!   [ EOS marker ]
//! with per-partition (start, end) byte ranges recorded so the Flight server can serve
//! a single output partition without reading neighbours.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;

use crate::shuffle_cache::{PartitionCache, chunk_target_bytes, partition_ref_id};

/// 1 MiB BufWriter capacity — amortizes syscall cost across multiple
/// small IPC writes per stripe. `StreamWriter::write` issues several
/// `write` calls per batch (continuation marker, metadata flatbuffer,
/// padding, then each column buffer); at sub-256-KiB stripes these
/// were going straight to the kernel as separate syscalls.
const FILE_BUF_BYTES: usize = 1024 * 1024;

struct CountingFile {
    inner: BufWriter<File>,
    bytes_written: u64,
}

impl CountingFile {
    fn new(inner: File) -> Self {
        Self {
            inner: BufWriter::with_capacity(FILE_BUF_BYTES, inner),
            bytes_written: 0,
        }
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

/// Write `partitions_per_output` to a single combined IPC file and emit one
/// `PartitionCache` per output partition with byte ranges.
///
/// `partitions_per_output[i]` holds all MicroPartitions destined for output partition `i`
/// across every worker state. Each output partition is concat'd, IPC-encoded, and appended
/// to the file in order; empty partitions are emitted as empty PartitionCache entries.
pub async fn write_partitions_one_shot(
    input_id: u32,
    shuffle_id: u64,
    shuffle_dirs: &[String],
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    partitions_per_output: Vec<Vec<MicroPartition>>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitions_per_output.len();

    // Concat + IPC encode + disk write all run on a single spawn_blocking thread.
    // Previously we fanned out per-partition `tokio::spawn` calls for the concat
    // phase, but at N=8192 partitions per map task that was 1.6M task allocations
    // whose scheduling overhead exceeded the actual concat work — and the M×N×data
    // sweep (see benchmarking/SHUFFLE_BENCH_FINDINGS.md) confirmed serial inline
    // is a strict improvement across all shapes including map_conc=1.
    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);
    let schema_for_write = schema.clone();
    let chunk_target = chunk_target_bytes();

    let (caches, _total_file_bytes) = get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<(Vec<PartitionCache>, u64)> {
            let mut partitions_per_output = partitions_per_output;
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let file = File::create(&file_path)?;
            let counting = CountingFile::new(file);
            let arrow_schema = Arc::new(schema_for_write.to_arrow()?);
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| {
                    DaftError::InternalError(format!("IPC compression init failed: {}", e))
                })?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                counting,
                arrow_schema.as_ref(),
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            // Emit one IPC batch per underlying RecordBatch (skipping the big
            // `RecordBatch::concat` fuse — see `concat_one_partition`). The cache
            // records one byte range per partition that covers all K IPC messages;
            // read-side StreamReader iterates them transparently, and the read-side
            // concat path can now combine them into properly-sized chunks instead
            // of receiving one giant pre-fused batch.
            //
            // One `Arc<OnceLock>` per map task, shared across every emitted cache
            // for this writer: the file is opened lazily by the first reader and
            // cached for every subsequent read of any partition in this file.
            let file_slot: Arc<std::sync::OnceLock<Arc<std::fs::File>>> =
                Arc::new(std::sync::OnceLock::new());
            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for (idx, slot_in) in partitions_per_output
                .iter_mut()
                .enumerate()
                .take(num_partitions)
            {
                let parts = std::mem::take(slot_in);
                let slot = concat_one_partition(parts, chunk_target, &arrow_schema)?;
                let ref_id = partition_ref_id(input_id, idx);
                match slot {
                    None => {
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema_for_write.clone(),
                            bytes_per_file: Vec::new(),
                            file_paths: Vec::new(),
                            file_slots: Vec::new(),
                            num_rows: 0,
                            size_bytes: 0,
                            byte_ranges: Some(Vec::new()),
                        });
                    }
                    Some((rows, bytes, arrow_batches)) => {
                        let offset_before = writer.get_ref().bytes_written;
                        for batch in &arrow_batches {
                            writer.write(batch).map_err(|e| {
                                DaftError::InternalError(format!("IPC write failed: {}", e))
                            })?;
                        }
                        let offset_after = writer.get_ref().bytes_written;
                        let batch_len = (offset_after - offset_before) as usize;
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema_for_write.clone(),
                            bytes_per_file: vec![batch_len],
                            file_paths: vec![file_path.clone()],
                            file_slots: vec![file_slot.clone()],
                            num_rows: rows,
                            size_bytes: bytes,
                            byte_ranges: Some(vec![(offset_before, offset_after)]),
                        });
                    }
                }
            }

            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            // `finish` writes the EOS marker through the BufWriter but does not
            // flush. Force-flush to surface errors here rather than swallowing
            // them via BufWriter::drop.
            writer
                .flush()
                .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))?;
            let total = writer.get_ref().bytes_written;
            Ok((caches, total))
        })
        .await??;

    Ok(caches)
}

/// Gather one output partition's MicroPartitions and split them into a sequence
/// of arrow `RecordBatch`es each ~`chunk_target_bytes` in size.
///
/// Algorithm mirrors the server-side read-concat path: walk the underlying
/// `RecordBatch`es, accumulate into a pending buffer, flush a fused chunk
/// once pending ≥ target. Single-RB pending vectors skip the fuse entirely
/// (pass-through). This adapts naturally to partition size:
///   - low-N / big per-RB: each RB is already ≥ target → emit as-is, zero fuse work
///   - high-N / tiny per-RB: everything stays under target → fuse once at end
///   - middle: combine small siblings up to target
fn concat_one_partition(
    parts: Vec<MicroPartition>,
    chunk_target_bytes: usize,
    arrow_schema: &Arc<arrow_schema::Schema>,
) -> DaftResult<Option<(usize, usize, Vec<arrow_array::RecordBatch>)>> {
    // Fast-path: no inputs at all (worker never received data). Avoid `MicroPartition::concat`,
    // which errors on empty input.
    if parts.is_empty() {
        return Ok(None);
    }
    let total_rows: usize = parts.iter().map(|p| p.len()).sum();
    if total_rows == 0 {
        return Ok(None);
    }
    let size_bytes: usize = parts.iter().map(|p| p.size_bytes()).sum();
    let combined = MicroPartition::concat(parts)?;
    let rbs = combined.record_batches();
    if rbs.is_empty() {
        return Ok(None);
    }
    let mut arrow_batches: Vec<arrow_array::RecordBatch> = Vec::new();
    let mut pending: Vec<&RecordBatch> = Vec::new();
    let mut pending_bytes: usize = 0;
    for rb in rbs {
        pending.push(rb);
        pending_bytes += rb.size_bytes();
        if pending_bytes >= chunk_target_bytes {
            flush_pending(&mut pending, &mut arrow_batches, arrow_schema)?;
            pending_bytes = 0;
        }
    }
    if !pending.is_empty() {
        flush_pending(&mut pending, &mut arrow_batches, arrow_schema)?;
    }
    Ok(Some((total_rows, size_bytes, arrow_batches)))
}

/// Build one `arrow_array::RecordBatch` from `pending` using the pre-computed
/// `arrow_schema`. Avoids the per-call `Schema::to_arrow` rebuild that `RecordBatch::try_into`
/// would otherwise pay — that rebuild was N·schema_fields allocations per partition and showed
/// up in `try_into_us` at ~44 µs per non-empty partition. Direct `RecordBatch::try_new` with
/// the cached schema also skips the validation/dtype-match path on the shared schema.
fn flush_pending(
    pending: &mut Vec<&RecordBatch>,
    out: &mut Vec<arrow_array::RecordBatch>,
    arrow_schema: &Arc<arrow_schema::Schema>,
) -> DaftResult<()> {
    let daft_batch_owned;
    let daft_batch: &RecordBatch = if pending.len() == 1 {
        pending[0]
    } else {
        daft_batch_owned = RecordBatch::concat(pending.as_slice())?;
        &daft_batch_owned
    };
    let columns = daft_batch
        .columns()
        .iter()
        .map(|c| c.as_materialized_series().to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;
    let arrow_batch = arrow_array::RecordBatch::try_new(arrow_schema.clone(), columns)
        .map_err(DaftError::ArrowRsError)?;
    out.push(arrow_batch);
    pending.clear();
    Ok(())
}
