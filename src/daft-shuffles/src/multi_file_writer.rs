//! Per-(map_task, output_partition) IPC writer for the Flight repartition path.
//!
//! Each map task writes N separate files — one per output partition — each containing
//! its own IPC schema header, a single concat'd batch for that partition, and an EOS
//! marker. Files are named `map_{input_id}_part_{partition_idx}.arrow`.
//!
//! Compared to `oneshot_writer` (one combined file per map task, N partitions delimited
//! by byte ranges), this trades file-count for read simplicity: every read is a whole-file
//! read with no byte-range bookkeeping. Compared to `partition_file_writer` (append to
//! a shared per-partition file), this restores per-task isolation — a failed write only
//! affects that one (map_task, partition) file.
//!
//! On-disk layout per file:
//!   [ IPC schema header ] [ batch ] [ EOS marker ]
//! The whole file is a complete `StreamWriter` output — `StreamReader::try_new` can
//! consume it directly. PartitionCache uses `byte_ranges: None` (`FileReadSpec::Whole`).

use std::{
    fs::File,
    io::{self, Write},
    path::Path,
    sync::atomic::Ordering,
};

use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_schema::schema::SchemaRef;

use crate::{
    multi_partition_cache::agg,
    shuffle_cache::{PartitionCache, partition_ref_id},
};

/// Concat each output partition's `Vec<MicroPartition>` in parallel, then write each
/// non-empty partition to its own IPC file. Returns one `PartitionCache` per output
/// partition with `byte_ranges: None`.
pub async fn write_partitions_multi_file(
    input_id: u32,
    shuffle_id: u64,
    shuffle_dirs: &[String],
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    partitions_per_output: Vec<Vec<MicroPartition>>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitions_per_output.len();

    // Phase 1 (parallel): concat each output partition's MicroPartitions into a single
    // arrow batch. Empty partitions stay as None.
    let mut concat_futs = Vec::with_capacity(num_partitions);
    for parts in partitions_per_output {
        concat_futs.push(tokio::spawn(async move { concat_one_partition(parts) }));
    }
    let concated_results = futures::future::join_all(concat_futs).await;
    let mut concated_per_partition: Vec<Option<(usize, usize, arrow_array::RecordBatch)>> =
        Vec::with_capacity(num_partitions);
    for jr in concated_results {
        let inner = jr.map_err(|e| DaftError::InternalError(e.to_string()))?;
        concated_per_partition.push(inner?);
    }

    // Phase 2 (sequential per-task, parallel across actors): write each partition's
    // batch to its own complete IPC file on spawn_blocking. Sequential within the
    // task because we want to avoid spawning N=512 blocking threads per finalize;
    // the underlying disk bandwidth is the bottleneck regardless of order.
    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);
    let schema_for_write = schema.clone();

    let (caches, total_bytes) = get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<(Vec<PartitionCache>, u64)> {
            if !Path::new(&shuffle_dir).exists() {
                std::fs::create_dir_all(&shuffle_dir)?;
            }
            let arrow_schema = schema_for_write.to_arrow()?;
            let write_options = IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| DaftError::InternalError(format!("IPC compression init failed: {}", e)))?;
            let mut caches = Vec::with_capacity(num_partitions);
            let mut total: u64 = 0;
            for (partition_idx, slot) in concated_per_partition.into_iter().enumerate() {
                let ref_id = partition_ref_id(input_id, partition_idx);
                let (num_rows, size_bytes, file_paths, bytes_per_file) = match slot {
                    Some((rows, bytes, arrow_batch)) => {
                        let file_path = format!(
                            "{}/map_{}_part_{}.arrow",
                            shuffle_dir, input_id, partition_idx
                        );
                        let file = File::create(&file_path)?;
                        let counting = CountingFile::new(file);
                        let mut writer = StreamWriter::try_new_with_options(
                            counting,
                            &arrow_schema,
                            write_options.clone(),
                        )
                        .map_err(|e| {
                            DaftError::InternalError(format!("IPC writer init failed: {}", e))
                        })?;
                        writer.write(&arrow_batch).map_err(|e| {
                            DaftError::InternalError(format!("IPC write failed: {}", e))
                        })?;
                        writer.finish().map_err(|e| {
                            DaftError::InternalError(format!("IPC writer finish failed: {}", e))
                        })?;
                        let file_bytes = writer.get_ref().bytes_written;
                        total += file_bytes;
                        (rows, bytes, vec![file_path], vec![file_bytes as usize])
                    }
                    // Empty partition — no file. The Flight server's `whole_specs` path
                    // tolerates zero files; downstream readers see no batches for this ref.
                    None => (0, 0, Vec::new(), Vec::new()),
                };
                caches.push(PartitionCache {
                    partition_ref_id: ref_id,
                    schema: schema_for_write.clone(),
                    bytes_per_file,
                    file_paths,
                    num_rows,
                    size_bytes,
                    // None signals whole-file reads on the server side (FileReadSpec::Whole)
                    // — distinct from `Some(Vec::new())` which means "empty range list".
                    byte_ranges: None,
                });
            }
            Ok((caches, total))
        })
        .await??;

    agg::CACHES_CLOSED.fetch_add(1, Ordering::Relaxed);
    agg::FILES_PRODUCED.fetch_add(caches.iter().filter(|c| !c.file_paths.is_empty()).count() as u64, Ordering::Relaxed);
    agg::OUTPUT_BYTES.fetch_add(total_bytes, Ordering::Relaxed);
    Ok(caches)
}

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

fn concat_one_partition(
    parts: Vec<MicroPartition>,
) -> DaftResult<Option<(usize, usize, arrow_array::RecordBatch)>> {
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
            Ok(Some((total_rows, size_bytes, arrow_batch)))
        }
        None => Ok(None),
    }
}
