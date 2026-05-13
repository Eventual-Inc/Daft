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
    io::{self, Write},
    path::Path,
    sync::atomic::Ordering,
    time::Instant,
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_schema::schema::SchemaRef;

use crate::{
    multi_partition_cache::{agg, record_flush_size, write_agg},
    shuffle_cache::{PartitionCache, partition_ref_id},
};

struct CountingFile {
    inner: File,
    bytes_written: u64,
}

impl CountingFile {
    fn new(inner: File) -> Self {
        Self { inner, bytes_written: 0 }
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

    let (caches, total_file_bytes) = get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<(Vec<PartitionCache>, u64)> {
            let t_block = Instant::now();
            let mut partitions_per_output = partitions_per_output;
            write_agg::SPAWN_TASKS.fetch_add(num_partitions as u64, Ordering::Relaxed);
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let file = File::create(&file_path)?;
            let counting = CountingFile::new(file);
            let arrow_schema = schema_for_write.to_arrow()?;
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| DaftError::InternalError(format!("IPC compression init failed: {}", e)))?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                counting,
                &arrow_schema,
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            // One IPC batch per output partition. Empty partitions emit an empty
            // cache entry (no byte range, no file path) so the read side resolves
            // the ref_id to "no data" without an open.
            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for idx in 0..num_partitions {
                let parts = std::mem::take(&mut partitions_per_output[idx]);
                let t_s = Instant::now();
                let slot = concat_one_partition(parts)?;
                write_agg::SPAWN_TOTAL_US
                    .fetch_add(t_s.elapsed().as_micros() as u64, Ordering::Relaxed);
                let ref_id = partition_ref_id(input_id, idx);
                match slot {
                    None => {
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema_for_write.clone(),
                            bytes_per_file: Vec::new(),
                            file_paths: Vec::new(),
                            num_rows: 0,
                            size_bytes: 0,
                            byte_ranges: Some(Vec::new()),
                        });
                    }
                    Some((rows, bytes, arrow_batch)) => {
                        let t_w = Instant::now();
                        let offset_before = writer.get_ref().bytes_written;
                        writer.write(&arrow_batch).map_err(|e| {
                            DaftError::InternalError(format!("IPC write failed: {}", e))
                        })?;
                        let offset_after = writer.get_ref().bytes_written;
                        write_agg::IPC_WRITE_US
                            .fetch_add(t_w.elapsed().as_micros() as u64, Ordering::Relaxed);
                        write_agg::FILE_WRITE_BYTES
                            .fetch_add(offset_after - offset_before, Ordering::Relaxed);
                        record_flush_size(offset_after - offset_before);
                        let batch_len = (offset_after - offset_before) as usize;
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema_for_write.clone(),
                            bytes_per_file: vec![batch_len],
                            file_paths: vec![file_path.clone()],
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
            let total = writer.get_ref().bytes_written;
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

/// Concat one output partition's MicroPartitions into a single arrow RecordBatch.
/// Returns `(num_rows, size_bytes, batch)` if non-empty, else `None`.
fn concat_one_partition(
    parts: Vec<MicroPartition>,
) -> DaftResult<Option<(usize, usize, arrow_array::RecordBatch)>> {
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
    let t_c = Instant::now();
    let combined = MicroPartition::concat(parts)?;
    write_agg::MP_CONCAT_US
        .fetch_add(t_c.elapsed().as_micros() as u64, Ordering::Relaxed);
    let t_g = Instant::now();
    let concated = combined.concat_or_get()?;
    write_agg::MP_CONCAT_OR_GET_US
        .fetch_add(t_g.elapsed().as_micros() as u64, Ordering::Relaxed);
    match concated {
        Some(rb) => {
            let t_t = Instant::now();
            let arrow_batch: arrow_array::RecordBatch = rb.try_into()?;
            write_agg::TRY_INTO_US
                .fetch_add(t_t.elapsed().as_micros() as u64, Ordering::Relaxed);
            write_agg::SPAWN_TASKS_NONEMPTY.fetch_add(1, Ordering::Relaxed);
            agg::INPUT_BYTES.fetch_add(size_bytes as u64, Ordering::Relaxed);
            Ok(Some((total_rows, size_bytes, arrow_batch)))
        }
        None => Ok(None),
    }
}
