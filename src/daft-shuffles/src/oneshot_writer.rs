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
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;

use crate::shuffle_cache::{PartitionCache, chunk_target_bytes, partition_ref_id};

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
            // Reused across partitions to avoid one Vec allocation per output.
            let mut arrow_batches_buf: Vec<arrow_array::RecordBatch> = Vec::with_capacity(1);
            for (idx, slot_in) in partitions_per_output.iter_mut().enumerate() {
                let parts = std::mem::take(slot_in);
                let ref_id = partition_ref_id(input_id, idx);
                match concat_one_partition(
                    parts,
                    chunk_target,
                    &arrow_schema,
                    &mut arrow_batches_buf,
                )? {
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
                        let offset_before = writer.get_ref().bytes_written;
                        for batch in &arrow_batches_buf {
                            writer.write(batch).map_err(|e| {
                                DaftError::InternalError(format!("IPC write failed: {}", e))
                            })?;
                        }
                        let offset_after = writer.get_ref().bytes_written;
                        let batch_len = (offset_after - offset_before) as usize;
                        caches.push(PartitionCache {
                            partition_ref_id: ref_id,
                            schema: schema.clone(),
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
            // BufWriter::drop swallows flush errors — surface them explicitly.
            writer
                .flush()
                .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))?;
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
) -> DaftResult<Option<(usize, usize)>> {
    out.clear();
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
    // Fast path: total fits in one chunk — skip per-rb size_bytes walk and emit a single fused batch.
    if size_bytes < chunk_target_bytes {
        let mut pending: Vec<&RecordBatch> = rbs.iter().collect();
        flush_pending(&mut pending, out, arrow_schema)?;
        return Ok(Some((total_rows, size_bytes)));
    }
    let mut pending: Vec<&RecordBatch> = Vec::with_capacity(rbs.len());
    let mut pending_bytes: usize = 0;
    for rb in rbs {
        pending.push(rb);
        pending_bytes += rb.size_bytes();
        if pending_bytes >= chunk_target_bytes {
            flush_pending(&mut pending, out, arrow_schema)?;
            pending_bytes = 0;
        }
    }
    if !pending.is_empty() {
        flush_pending(&mut pending, out, arrow_schema)?;
    }
    Ok(Some((total_rows, size_bytes)))
}

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
