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
    compression: Option<arrow_ipc::CompressionType>,
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

    // Write-side coalescing knob: group K adjacent output partitions into a
    // single IPC batch. Amortizes per-message fixed cost (flatbuffer build +
    // syscall + Flight per-message tax on the read side) across K partitions.
    // K=1 (default) preserves the legacy "one batch per partition" layout —
    // each cache entry's row_range is None.
    let coalesce_k: usize = std::env::var("DAFT_SHUFFLE_WRITE_COALESCE_K")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&k: &usize| k >= 1)
        .unwrap_or(1);

    let (caches, total_file_bytes) = get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<(Vec<PartitionCache>, u64)> {
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let file = File::create(&file_path)?;
            let counting = CountingFile::new(file);
            let arrow_schema = schema_for_write.to_arrow()?;
            let arrow_schema_ref: arrow_schema::SchemaRef = std::sync::Arc::new(arrow_schema.clone());
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| DaftError::InternalError(format!("IPC compression init failed: {}", e)))?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                counting,
                &arrow_schema,
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            // Process partitions in groups of `coalesce_k`. Each group emits one
            // IPC batch; per-partition cache entries share the byte range but
            // record their own row_range slice within the batch.
            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            let mut p = 0usize;
            while p < num_partitions {
                let g_end = (p + coalesce_k).min(num_partitions);

                // Gather non-empty slots for this group, recording each
                // partition's row offset within the merged batch.
                let mut group_batches: Vec<arrow_array::RecordBatch> = Vec::with_capacity(g_end - p);
                let mut per_partition_rows: Vec<usize> = Vec::with_capacity(g_end - p);
                let mut per_partition_bytes: Vec<usize> = Vec::with_capacity(g_end - p);
                let mut group_total_rows: usize = 0;
                let mut group_total_size: usize = 0;
                // index in `caches` array for partitions [p..g_end). Filled below
                // after we know the byte range.
                let mut slots_in_group: Vec<Option<(usize, usize)>> =
                    Vec::with_capacity(g_end - p);
                for idx_in_group in 0..(g_end - p) {
                    let slot = concated_per_partition[p + idx_in_group].take();
                    match slot {
                        Some((rows, bytes, arrow_batch)) => {
                            slots_in_group.push(Some((rows, bytes)));
                            per_partition_rows.push(rows);
                            per_partition_bytes.push(bytes);
                            group_batches.push(arrow_batch);
                            group_total_rows += rows;
                            group_total_size += bytes;
                        }
                        None => {
                            slots_in_group.push(None);
                            per_partition_rows.push(0);
                            per_partition_bytes.push(0);
                        }
                    }
                }

                if group_batches.is_empty() {
                    // Whole group is empty: emit empty caches for each partition.
                    for idx_in_group in 0..(g_end - p) {
                        let ref_id = partition_ref_id(input_id, p + idx_in_group);
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema_for_write.clone(),
                            bytes_per_file: Vec::new(),
                            file_paths: Vec::new(),
                            num_rows: 0,
                            size_bytes: 0,
                            byte_ranges: Some(Vec::new()),
                            row_ranges: None,
                        });
                    }
                } else {
                    let merged = arrow_select::concat::concat_batches(
                        &arrow_schema_ref,
                        group_batches.iter(),
                    )
                    .map_err(|e| DaftError::InternalError(format!("write-side concat: {}", e)))?;

                    let offset_before = writer.get_ref().bytes_written;
                    writer.write(&merged).map_err(|e| {
                        DaftError::InternalError(format!("IPC write failed: {}", e))
                    })?;
                    let offset_after = writer.get_ref().bytes_written;
                    record_flush_size(offset_after - offset_before);
                    let batch_byte_range = (offset_before, offset_after);
                    let batch_len = (offset_after - offset_before) as usize;

                    // Walk per-partition row offsets within the merged batch.
                    let mut cum_rows: u64 = 0;
                    let mut group_input_iter = 0usize; // index into the contributors of this group
                    for idx_in_group in 0..(g_end - p) {
                        let ref_id = partition_ref_id(input_id, p + idx_in_group);
                        match slots_in_group[idx_in_group] {
                            Some((rows, _bytes)) => {
                                // This partition is the next contributor in group_batches.
                                let row_start = cum_rows;
                                let row_end = cum_rows + rows as u64;
                                cum_rows = row_end;
                                group_input_iter += 1;
                                // Single-partition group: no row slicing needed
                                let row_ranges = if coalesce_k == 1 || (g_end - p) == 1 {
                                    None
                                } else {
                                    Some(vec![(row_start, row_end)])
                                };
                                caches.push(PartitionCache {
                                    partition_ref_id: ref_id,
                                    schema: schema_for_write.clone(),
                                    bytes_per_file: vec![batch_len],
                                    file_paths: vec![file_path.clone()],
                                    num_rows: rows,
                                    size_bytes: per_partition_bytes[idx_in_group],
                                    byte_ranges: Some(vec![batch_byte_range]),
                                    row_ranges,
                                });
                            }
                            None => {
                                // Empty partition within a non-empty group.
                                // Emit a row_range that's zero-width at cum_rows so
                                // the read side can still resolve the ref_id.
                                let row_ranges = if coalesce_k == 1 {
                                    None
                                } else {
                                    Some(vec![(cum_rows, cum_rows)])
                                };
                                caches.push(PartitionCache {
                                    partition_ref_id: ref_id,
                                    schema: schema_for_write.clone(),
                                    bytes_per_file: vec![batch_len],
                                    file_paths: vec![file_path.clone()],
                                    num_rows: 0,
                                    size_bytes: 0,
                                    byte_ranges: Some(vec![batch_byte_range]),
                                    row_ranges,
                                });
                            }
                        }
                    }
                    let _ = group_input_iter;
                }

                p = g_end;
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
