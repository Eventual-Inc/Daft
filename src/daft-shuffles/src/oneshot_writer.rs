//! One-shot combined-file shuffle writer for the Flight backend.
//!
//! Writes all output partitions of a single map task into one IPC stream file:
//!   [ schema header ] [ partition 0 batches ] ... [ partition N-1 batches ] [ EOS ]
//! Per-partition (start, end) byte ranges let the Flight server serve a single
//! output partition without scanning neighbours.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;

use crate::shuffle_cache::{PartitionCache, chunk_target_bytes, partition_ref_id};

const FILE_BUF_BYTES: usize = 1024 * 1024;

#[derive(Default)]
struct MapTaskProfile {
    t_concat_one_total: Duration,
    t_input_rollup: Duration,
    t_mp_concat: Duration,
    t_arrow_concat: Duration,
    t_to_arrow: Duration,
    t_pcache_build: Duration,
    t_ipc_write: Duration,
    t_ipc_finish: Duration,
    num_partitions: usize,
    num_nonempty_partitions: usize,
    num_arrow_batches: usize,
    num_input_mps: usize,
    num_input_rbs: usize,
    total_rows: usize,
    total_input_bytes: usize,
    total_written_bytes: u64,
}

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

/// Write `partitions_per_output` to a single combined IPC file. Returns one
/// `PartitionCache` per output partition with the byte range to read.
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
    let chunk_target = chunk_target_bytes();

    get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<Vec<PartitionCache>> {
            let t_total = Instant::now();
            let mut prof = MapTaskProfile {
                num_partitions,
                ..Default::default()
            };

            let mut partitions_per_output = partitions_per_output;
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let file = File::create(&file_path)?;
            let counting = CountingFile::new(file);
            let arrow_schema = Arc::new(schema.to_arrow()?);
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

            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            // Reused across partitions to avoid 2048 Vec allocations.
            let mut arrow_batches_buf: Vec<arrow_array::RecordBatch> = Vec::with_capacity(1);
            for (idx, slot_in) in partitions_per_output.iter_mut().enumerate() {
                let parts = std::mem::take(slot_in);
                let ref_id = partition_ref_id(input_id, idx);
                let t_cop = Instant::now();
                let cop_result = concat_one_partition(
                    parts,
                    chunk_target,
                    &arrow_schema,
                    &mut arrow_batches_buf,
                    &mut prof,
                )?;
                prof.t_concat_one_total += t_cop.elapsed();
                match cop_result {
                    None => {
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema.clone(),
                            bytes_per_file: Vec::new(),
                            file_paths: Vec::new(),
                            num_rows: 0,
                            size_bytes: 0,
                            byte_ranges: Some(Vec::new()),
                        });
                    }
                    Some((rows, bytes)) => {
                        prof.num_nonempty_partitions += 1;
                        prof.num_arrow_batches += arrow_batches_buf.len();
                        prof.total_rows += rows;
                        prof.total_input_bytes += bytes;
                        let offset_before = writer.get_ref().bytes_written;
                        let t_write = Instant::now();
                        for batch in &arrow_batches_buf {
                            writer.write(batch).map_err(|e| {
                                DaftError::InternalError(format!("IPC write failed: {}", e))
                            })?;
                        }
                        prof.t_ipc_write += t_write.elapsed();
                        let offset_after = writer.get_ref().bytes_written;
                        let batch_len = (offset_after - offset_before) as usize;
                        let t_pcache = Instant::now();
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema.clone(),
                            bytes_per_file: vec![batch_len],
                            file_paths: vec![file_path.clone()],
                            num_rows: rows,
                            size_bytes: bytes,
                            byte_ranges: Some(vec![(offset_before, offset_after)]),
                        });
                        prof.t_pcache_build += t_pcache.elapsed();
                    }
                }
            }

            let t_finish = Instant::now();
            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            // BufWriter::drop swallows flush errors — surface them explicitly.
            writer
                .flush()
                .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))?;
            prof.t_ipc_finish = t_finish.elapsed();
            prof.total_written_bytes = writer.get_ref().bytes_written;

            let total_ms = t_total.elapsed().as_secs_f64() * 1e3;
            let concat_one_total_ms = prof.t_concat_one_total.as_secs_f64() * 1e3;
            let input_rollup_ms = prof.t_input_rollup.as_secs_f64() * 1e3;
            let mp_concat_ms = prof.t_mp_concat.as_secs_f64() * 1e3;
            let arrow_concat_ms = prof.t_arrow_concat.as_secs_f64() * 1e3;
            let to_arrow_ms = prof.t_to_arrow.as_secs_f64() * 1e3;
            let pcache_build_ms = prof.t_pcache_build.as_secs_f64() * 1e3;
            let ipc_write_ms = prof.t_ipc_write.as_secs_f64() * 1e3;
            let ipc_finish_ms = prof.t_ipc_finish.as_secs_f64() * 1e3;
            // Time inside concat_one_partition not in any sub-span.
            let concat_one_unattributed_ms = (concat_one_total_ms
                - input_rollup_ms
                - mp_concat_ms
                - arrow_concat_ms
                - to_arrow_ms)
                .max(0.0);
            // Time in the main loop body outside concat_one_partition / ipc_write / pcache_build,
            // plus one-shot costs (file create, schema serialize, writer init).
            let outer_loop_ms = (total_ms
                - concat_one_total_ms
                - ipc_write_ms
                - ipc_finish_ms
                - pcache_build_ms)
                .max(0.0);
            tracing::info!(
                target: "daft_shuffles::oneshot_writer::profile",
                input_id,
                shuffle_id,
                num_partitions = prof.num_partitions,
                num_nonempty = prof.num_nonempty_partitions,
                num_arrow_batches = prof.num_arrow_batches,
                num_input_mps = prof.num_input_mps,
                num_input_rbs = prof.num_input_rbs,
                total_rows = prof.total_rows,
                total_input_bytes = prof.total_input_bytes,
                total_written_bytes = prof.total_written_bytes,
                compressed = compression.is_some(),
                total_ms = format!("{:.2}", total_ms),
                concat_one_total_ms = format!("{:.2}", concat_one_total_ms),
                input_rollup_ms = format!("{:.2}", input_rollup_ms),
                mp_concat_ms = format!("{:.2}", mp_concat_ms),
                arrow_concat_ms = format!("{:.2}", arrow_concat_ms),
                to_arrow_ms = format!("{:.2}", to_arrow_ms),
                concat_one_unattributed_ms = format!("{:.2}", concat_one_unattributed_ms),
                pcache_build_ms = format!("{:.2}", pcache_build_ms),
                ipc_write_ms = format!("{:.2}", ipc_write_ms),
                ipc_finish_ms = format!("{:.2}", ipc_finish_ms),
                outer_loop_ms = format!("{:.2}", outer_loop_ms),
                "oneshot map task profile"
            );
            Ok(caches)
        })
        .await?
}

/// Concat one output partition's MicroPartitions, then split into arrow batches
/// each ≥ `chunk_target_bytes`. Single-batch pending groups skip the fuse.
/// Appends output arrow batches to `out` (caller-owned, cleared at entry).
fn concat_one_partition(
    parts: Vec<MicroPartition>,
    chunk_target_bytes: usize,
    arrow_schema: &Arc<arrow_schema::Schema>,
    out: &mut Vec<arrow_array::RecordBatch>,
    prof: &mut MapTaskProfile,
) -> DaftResult<Option<(usize, usize)>> {
    out.clear();
    if parts.is_empty() {
        return Ok(None);
    }
    let t_roll = Instant::now();
    let total_rows: usize = parts.iter().map(|p| p.len()).sum();
    if total_rows == 0 {
        prof.t_input_rollup += t_roll.elapsed();
        return Ok(None);
    }
    let size_bytes: usize = parts.iter().map(|p| p.size_bytes()).sum();
    prof.t_input_rollup += t_roll.elapsed();
    prof.num_input_mps += parts.len();
    let t_mp = Instant::now();
    let combined = MicroPartition::concat(parts)?;
    prof.t_mp_concat += t_mp.elapsed();
    let rbs = combined.record_batches();
    prof.num_input_rbs += rbs.len();
    if rbs.is_empty() {
        return Ok(None);
    }
    // Fast path: total fits in one chunk — skip per-rb size_bytes walk and emit a single fused batch.
    if size_bytes < chunk_target_bytes {
        let mut pending: Vec<&RecordBatch> = rbs.iter().collect();
        flush_pending(&mut pending, out, arrow_schema, prof)?;
        return Ok(Some((total_rows, size_bytes)));
    }
    let mut pending: Vec<&RecordBatch> = Vec::with_capacity(rbs.len());
    let mut pending_bytes: usize = 0;
    for rb in rbs {
        pending.push(rb);
        pending_bytes += rb.size_bytes();
        if pending_bytes >= chunk_target_bytes {
            flush_pending(&mut pending, out, arrow_schema, prof)?;
            pending_bytes = 0;
        }
    }
    if !pending.is_empty() {
        flush_pending(&mut pending, out, arrow_schema, prof)?;
    }
    Ok(Some((total_rows, size_bytes)))
}

fn flush_pending(
    pending: &mut Vec<&RecordBatch>,
    out: &mut Vec<arrow_array::RecordBatch>,
    arrow_schema: &Arc<arrow_schema::Schema>,
    prof: &mut MapTaskProfile,
) -> DaftResult<()> {
    let daft_batch_owned;
    let daft_batch: &RecordBatch = if pending.len() == 1 {
        pending[0]
    } else {
        let t = Instant::now();
        daft_batch_owned = RecordBatch::concat(pending.as_slice())?;
        prof.t_arrow_concat += t.elapsed();
        &daft_batch_owned
    };
    let t = Instant::now();
    let columns = daft_batch
        .columns()
        .iter()
        .map(|c| c.as_materialized_series().to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;
    let arrow_batch = arrow_array::RecordBatch::try_new(arrow_schema.clone(), columns)
        .map_err(DaftError::ArrowRsError)?;
    prof.t_to_arrow += t.elapsed();
    out.push(arrow_batch);
    pending.clear();
    Ok(())
}
