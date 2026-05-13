//! One-shot combined-file shuffle writer.
//!
//! Used by `RepartitionSink` on the Flight backend. Callers accumulate per-output-partition
//! `Vec<MicroPartition>` in memory during `sink` (same as ray-plasma), and at finalize hand
//! the whole accumulator to `write_partitions_one_shot`. This does one `MicroPartition::concat`
//! + one IPC encode + one disk write per output partition — vs the streaming writer which paid
//! a concat-encode-write cycle every time a partition's buffer crossed COALESCE_THRESHOLD_BYTES.
//!
//! On-disk layout matches `MultiPartitionShuffleCache`:
//!   [ IPC schema header ]
//!   [ partition 0 batch ] [ partition 1 batch ] ... [ partition N-1 batch ]
//!   [ EOS marker ]
//! with per-partition (start, end) byte ranges recorded so the Flight server can serve
//! a single output partition without reading neighbours.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;

use crate::{
    multi_partition_cache::{agg, record_flush_size, write_agg},
    shuffle_cache::{PartitionCache, chunk_target_bytes, partition_ref_id},
};

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

    fn into_inner(self) -> io::Result<File> {
        self.inner.into_inner().map_err(io::Error::from)
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

    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);
    let schema_for_write = schema.clone();
    let chunk_target = chunk_target_bytes();

    let (caches, total_file_bytes) = get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<(Vec<PartitionCache>, u64)> {
            let t_block = Instant::now();
            let mut partitions_per_output = partitions_per_output;
            write_agg::PARTITIONS_TOUCHED.fetch_add(num_partitions as u64, Ordering::Relaxed);
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let file = File::create(&file_path)?;
            let counting = CountingFile::new(file);
            let arrow_schema = Arc::new(schema_for_write.to_arrow()?);
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| DaftError::InternalError(format!("IPC compression init failed: {}", e)))?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                counting,
                arrow_schema.as_ref(),
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            // One `Arc<OnceLock>` per map task; kept for back-compat with the
            // reader API but unused on the read path since the slot_or_open revert.
            let file_slot: Arc<std::sync::OnceLock<Arc<std::fs::File>>> =
                Arc::new(std::sync::OnceLock::new());

            // Per-partition: list of (chunk_byte_start, chunk_byte_end, row_start_in_chunk,
            // row_end_in_chunk). A partition can contribute rows to multiple chunks.
            let mut p_chunks: Vec<Vec<(u64, u64, u64, u64)>> =
                (0..num_partitions).map(|_| Vec::new()).collect();
            let mut p_rows: Vec<usize> = vec![0; num_partitions];
            let mut p_bytes: Vec<usize> = vec![0; num_partitions];

            // Cross-partition pending buffer. When pending_bytes crosses chunk_target,
            // concat all pending batches and write ONE IPC message. Each entry of
            // pending_slices says which partition contributed which row range to the
            // currently-pending concatenation.
            let mut pending: Vec<arrow_array::RecordBatch> = Vec::new();
            let mut pending_bytes: usize = 0;
            let mut pending_row_offset: u64 = 0;
            let mut pending_slices: Vec<(usize, u64, u64)> = Vec::new();

            for pid in 0..num_partitions {
                let parts = std::mem::take(&mut partitions_per_output[pid]);
                let t_s = Instant::now();
                if parts.is_empty() {
                    write_agg::CONCAT_ONE_PARTITION_US
                        .fetch_add(t_s.elapsed().as_micros() as u64, Ordering::Relaxed);
                    continue;
                }
                let total_rows: usize = parts.iter().map(|p| p.len()).sum();
                if total_rows == 0 {
                    write_agg::CONCAT_ONE_PARTITION_US
                        .fetch_add(t_s.elapsed().as_micros() as u64, Ordering::Relaxed);
                    continue;
                }
                let size_bytes: usize = parts.iter().map(|p| p.size_bytes()).sum();
                p_rows[pid] = total_rows;
                p_bytes[pid] = size_bytes;

                let t_c = Instant::now();
                let combined = MicroPartition::concat(parts)?;
                write_agg::MP_CONCAT_US
                    .fetch_add(t_c.elapsed().as_micros() as u64, Ordering::Relaxed);
                let rbs = combined.record_batches();
                if rbs.is_empty() {
                    write_agg::CONCAT_ONE_PARTITION_US
                        .fetch_add(t_s.elapsed().as_micros() as u64, Ordering::Relaxed);
                    continue;
                }

                // Within-partition adaptive accumulator: combine sibling RBs up to
                // chunk_target before converting to arrow. Mirrors the old
                // `concat_one_partition` behavior; output is a sequence of arrow
                // batches each <= chunk_target.
                let t_t = Instant::now();
                let mut per_partition_arrow: Vec<arrow_array::RecordBatch> = Vec::new();
                let mut wp_pending: Vec<&RecordBatch> = Vec::new();
                let mut wp_bytes: usize = 0;
                for rb in rbs {
                    wp_pending.push(rb);
                    wp_bytes += rb.size_bytes();
                    if wp_bytes >= chunk_target {
                        flush_pending(&mut wp_pending, &mut per_partition_arrow, &arrow_schema)?;
                        wp_bytes = 0;
                    }
                }
                if !wp_pending.is_empty() {
                    flush_pending(&mut wp_pending, &mut per_partition_arrow, &arrow_schema)?;
                }
                write_agg::TRY_INTO_US
                    .fetch_add(t_t.elapsed().as_micros() as u64, Ordering::Relaxed);
                write_agg::PARTITIONS_NONEMPTY.fetch_add(1, Ordering::Relaxed);
                agg::INPUT_BYTES.fetch_add(size_bytes as u64, Ordering::Relaxed);

                // Feed each per-partition arrow batch into the cross-partition
                // pending. Flush BEFORE adding when pending is already at or past
                // target so chunks stay close to the target size (vs growing
                // unbounded if many tiny tail batches arrive).
                for ab in per_partition_arrow {
                    if pending_bytes >= chunk_target && !pending.is_empty() {
                        cross_flush(
                            &mut writer,
                            &arrow_schema,
                            &mut pending,
                            &mut pending_bytes,
                            &mut pending_row_offset,
                            &mut pending_slices,
                            &mut p_chunks,
                        )?;
                    }
                    let nrows = ab.num_rows() as u64;
                    let bsize = crate::server::flight_server::estimate_batch_size(&ab);
                    let row_start = pending_row_offset;
                    let row_end = row_start + nrows;
                    pending.push(ab);
                    match pending_slices.last_mut() {
                        Some(entry) if entry.0 == pid => entry.2 = row_end,
                        _ => pending_slices.push((pid, row_start, row_end)),
                    }
                    pending_row_offset = row_end;
                    pending_bytes += bsize;
                }
                write_agg::CONCAT_ONE_PARTITION_US
                    .fetch_add(t_s.elapsed().as_micros() as u64, Ordering::Relaxed);
            }

            // Final cross-partition flush for whatever's left.
            if !pending.is_empty() {
                cross_flush(
                    &mut writer,
                    &arrow_schema,
                    &mut pending,
                    &mut pending_bytes,
                    &mut pending_row_offset,
                    &mut pending_slices,
                    &mut p_chunks,
                )?;
            }

            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            // `finish` writes the EOS marker through the BufWriter but does not
            // flush. Force-flush to surface errors here rather than swallowing
            // them via BufWriter::drop.
            writer.flush().map_err(|e| {
                DaftError::InternalError(format!("IPC writer flush failed: {}", e))
            })?;
            let total = writer.get_ref().bytes_written;

            // Build PartitionCaches. Each non-empty partition gets one entry per
            // chunk it contributed to; entries share the file path and slot.
            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for pid in 0..num_partitions {
                let ref_id = partition_ref_id(input_id, pid);
                let chunks = std::mem::take(&mut p_chunks[pid]);
                if chunks.is_empty() {
                    caches.push(PartitionCache {
                        partition_ref_id: ref_id,
                        schema: schema_for_write.clone(),
                        bytes_per_file: Vec::new(),
                        file_paths: Vec::new(),
                        file_slots: Vec::new(),
                        num_rows: 0,
                        size_bytes: 0,
                        byte_ranges: Some(Vec::new()),
                        row_ranges: Some(Vec::new()),
                    });
                } else {
                    let n = chunks.len();
                    let file_paths = vec![file_path.clone(); n];
                    let file_slots = vec![file_slot.clone(); n];
                    let mut bytes_per_file = Vec::with_capacity(n);
                    let mut byte_ranges = Vec::with_capacity(n);
                    let mut row_ranges = Vec::with_capacity(n);
                    for (start, end, rs, re) in chunks {
                        bytes_per_file.push((end - start) as usize);
                        byte_ranges.push((start, end));
                        row_ranges.push((rs, re));
                    }
                    caches.push(PartitionCache {
                        partition_ref_id: ref_id,
                        schema: schema_for_write.clone(),
                        bytes_per_file,
                        file_paths,
                        file_slots,
                        num_rows: p_rows[pid],
                        size_bytes: p_bytes[pid],
                        byte_ranges: Some(byte_ranges),
                        row_ranges: Some(row_ranges),
                    });
                }
            }
            write_agg::BLOCKING_WALL_US
                .fetch_add(t_block.elapsed().as_micros() as u64, Ordering::Relaxed);
            Ok((caches, total))
        })
        .await??;

    agg::CACHES_CLOSED.fetch_add(1, Ordering::Relaxed);
    agg::FILES_PRODUCED.fetch_add(1, Ordering::Relaxed);
    agg::OUTPUT_BYTES.fetch_add(total_file_bytes, Ordering::Relaxed);
    write_agg::ONESHOT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(caches)
}

/// Concat pending arrow batches into one big batch, IPC-write as a single
/// message, and record (byte_range, row_range) for each partition that
/// contributed. Clears `pending`, `pending_slices`, and resets offsets.
fn cross_flush(
    writer: &mut arrow_ipc::writer::StreamWriter<CountingFile>,
    arrow_schema: &Arc<arrow_schema::Schema>,
    pending: &mut Vec<arrow_array::RecordBatch>,
    pending_bytes: &mut usize,
    pending_row_offset: &mut u64,
    pending_slices: &mut Vec<(usize, u64, u64)>,
    p_chunks: &mut [Vec<(u64, u64, u64, u64)>],
) -> DaftResult<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let merged = if pending.len() == 1 {
        // Single-batch fast path: no concat allocation.
        pending.pop().unwrap()
    } else {
        let to_concat = std::mem::take(pending);
        arrow_select::concat::concat_batches(arrow_schema, to_concat.iter())
            .map_err(DaftError::ArrowRsError)?
    };
    let t_w = Instant::now();
    let offset_before = writer.get_ref().bytes_written;
    writer
        .write(&merged)
        .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
    let offset_after = writer.get_ref().bytes_written;
    write_agg::IPC_WRITE_US.fetch_add(t_w.elapsed().as_micros() as u64, Ordering::Relaxed);
    write_agg::FILE_WRITE_BYTES.fetch_add(offset_after - offset_before, Ordering::Relaxed);
    record_flush_size(offset_after - offset_before);
    for (pid, rs, re) in pending_slices.drain(..) {
        p_chunks[pid].push((offset_before, offset_after, rs, re));
    }
    pending.clear();
    *pending_bytes = 0;
    *pending_row_offset = 0;
    Ok(())
}

/// Build one `arrow_array::RecordBatch` from `pending` daft batches using the
/// pre-computed `arrow_schema`. Used within a partition to fuse small sibling
/// RBs up to chunk_target before they hit the cross-partition accumulator.
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
