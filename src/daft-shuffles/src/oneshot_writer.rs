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
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_schema::schema::SchemaRef;

use crate::{
    multi_partition_cache::{agg, record_flush_size},
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
    partitions_per_output: Vec<Vec<MicroPartition>>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitions_per_output.len();

    // Parallel concat: each output partition becomes Option<arrow_array::RecordBatch>
    // (None when the partition has no rows). Same pattern as the ray finalize path.
    let mut concat_futs = Vec::with_capacity(num_partitions);
    for parts in partitions_per_output {
        concat_futs.push(tokio::spawn(async move { concat_one_partition(parts) }));
    }
    let concated = futures::future::join_all(concat_futs).await;
    let mut concated_per_partition: Vec<Option<(usize, usize, arrow_array::RecordBatch)>> =
        Vec::with_capacity(num_partitions);
    for jr in concated {
        let inner = jr.map_err(|e| DaftError::InternalError(e.to_string()))?;
        concated_per_partition.push(inner?);
    }

    // Sequential write on spawn_blocking. Concat + IPC encode happen on the same thread
    // as the write, which is fine because we're I/O bound here and the concat work is
    // already done.
    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);
    let schema_for_write = schema.clone();

    let (caches, total_file_bytes) = get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<(Vec<PartitionCache>, u64)> {
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let file = File::create(&file_path)?;
            let counting = CountingFile::new(file);
            let arrow_schema = schema_for_write.to_arrow()?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                counting,
                &arrow_schema,
                arrow_ipc::writer::IpcWriteOptions::default(),
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            let mut caches = Vec::with_capacity(num_partitions);
            for (partition_idx, slot) in concated_per_partition.into_iter().enumerate() {
                let ref_id = partition_ref_id(input_id, partition_idx);
                let (num_rows, size_bytes, byte_ranges, file_paths, bytes_per_file) = match slot {
                    Some((rows, bytes, arrow_batch)) => {
                        let offset_before = writer.get_ref().bytes_written;
                        writer.write(&arrow_batch).map_err(|e| {
                            DaftError::InternalError(format!("IPC write failed: {}", e))
                        })?;
                        let offset_after = writer.get_ref().bytes_written;
                        record_flush_size(offset_after - offset_before);
                        (
                            rows,
                            bytes,
                            Some(vec![(offset_before, offset_after)]),
                            vec![file_path.clone()],
                            vec![(offset_after - offset_before) as usize],
                        )
                    }
                    None => (0, 0, Some(Vec::new()), Vec::new(), Vec::new()),
                };
                caches.push(PartitionCache {
                    partition_ref_id: ref_id,
                    schema: schema_for_write.clone(),
                    bytes_per_file,
                    file_paths,
                    num_rows,
                    size_bytes,
                    byte_ranges,
                });
            }
            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            let total = writer.get_ref().bytes_written;
            Ok((caches, total))
        })
        .await??;

    agg::CACHES_CLOSED.fetch_add(1, Ordering::Relaxed);
    agg::FILES_PRODUCED.fetch_add(1, Ordering::Relaxed);
    agg::OUTPUT_BYTES.fetch_add(total_file_bytes, Ordering::Relaxed);
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
    let combined = MicroPartition::concat(parts)?;
    let concated = combined.concat_or_get()?;
    match concated {
        Some(rb) => {
            let arrow_batch: arrow_array::RecordBatch = rb.try_into()?;
            agg::INPUT_BYTES.fetch_add(size_bytes as u64, Ordering::Relaxed);
            Ok(Some((total_rows, size_bytes, arrow_batch)))
        }
        None => Ok(None),
    }
}
