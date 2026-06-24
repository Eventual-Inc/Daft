//! One-shot combined-file shuffle writer for the Flight backend.
//!
//! Writes all output partitions of a single map task into one IPC stream file:
//!   [ schema header ] [ partition 0 batches ] ... [ partition N-1 batches ] [ EOS ]
//! Per-partition (start, end) byte ranges let the Flight server serve a single
//! output partition without scanning neighbours.

use std::{
    fs::File,
    io::{self, BufWriter, Write},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;

use crate::shuffle_cache::{CHUNK_TARGET_BYTES, PartitionCache, partition_ref_id};

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

type ShuffleWriter = arrow_ipc::writer::StreamWriter<CountingFile>;

/// Write `partitions` to a single combined IPC file. Returns one
/// `PartitionCache` per output partition with the byte range to read.
pub async fn write_partitions_one_shot(
    input_id: u32,
    shuffle_id: u64,
    shuffle_dirs: &[String],
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    partitions: Vec<MicroPartition>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitions.len();
    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);

    // IPC encode + disk write all run on a single spawn_blocking thread.
    // Previously we fanned out per-partition `tokio::spawn` calls, but at
    // N=8192 partitions per map task that was 1.6M task allocations whose
    // scheduling overhead exceeded the actual work.
    get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<Vec<PartitionCache>> {
            std::fs::create_dir_all(&shuffle_dir)?;
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let arrow_schema = Arc::new(schema.to_arrow()?);
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| {
                    DaftError::InternalError(format!("IPC compression init failed: {}", e))
                })?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                CountingFile::new(File::create(&file_path)?),
                arrow_schema.as_ref(),
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for (idx, partition) in partitions.into_iter().enumerate() {
                caches.push(write_one_partition(
                    partition,
                    partition_ref_id(input_id, idx),
                    &mut writer,
                    &arrow_schema,
                    &schema,
                    &file_path,
                )?);
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

/// Write one output partition to `writer`, coalescing small Daft record batches
/// up to `CHUNK_TARGET_BYTES` per IPC message. Large batches pass through unsplit.
///
/// Adapts naturally to partition shape:
///   - low-N / big per-RB: each RB is already ≥ target → emit as-is, zero fuse work
///   - high-N / tiny per-RB: everything stays under target → fuse once at end
///   - middle: combine small siblings up to target
fn write_one_partition(
    partition: MicroPartition,
    ref_id: u64,
    writer: &mut ShuffleWriter,
    arrow_schema: &Arc<arrow_schema::Schema>,
    schema: &SchemaRef,
    file_path: &str,
) -> DaftResult<PartitionCache> {
    let num_rows = partition.len();
    if num_rows == 0 {
        return Ok(PartitionCache {
            partition_ref_id: ref_id,
            schema: schema.clone(),
            bytes_per_file: Vec::new(),
            file_paths: Vec::new(),
            num_rows: 0,
            size_bytes: 0,
            byte_ranges: Some(Vec::new()),
        });
    }
    let batches = partition.record_batches();

    let offset_before = writer.get_ref().bytes_written;
    let mut size_bytes = 0;
    let mut group_start = 0;
    let mut group_bytes = 0;
    for (i, batch) in batches.iter().enumerate() {
        let b = batch.size_bytes();
        size_bytes += b;
        group_bytes += b;
        if group_bytes >= CHUNK_TARGET_BYTES {
            write_coalesced(&batches[group_start..=i], writer, arrow_schema)?;
            group_start = i + 1;
            group_bytes = 0;
        }
    }
    if group_start < batches.len() {
        write_coalesced(&batches[group_start..], writer, arrow_schema)?;
    }
    let offset_after = writer.get_ref().bytes_written;

    Ok(PartitionCache {
        partition_ref_id: ref_id,
        schema: schema.clone(),
        bytes_per_file: vec![(offset_after - offset_before) as usize],
        file_paths: vec![file_path.to_string()],
        num_rows,
        size_bytes,
        byte_ranges: Some(vec![(offset_before, offset_after)]),
    })
}

/// Fuse `batches` into one `arrow_array::RecordBatch` and write it. Uses the
/// pre-computed `arrow_schema` to avoid the per-call `Schema::to_arrow` rebuild
/// that `RecordBatch::try_into` would otherwise pay — that rebuild is
/// N·schema_fields allocations per partition.
fn write_coalesced(
    batches: &[RecordBatch],
    writer: &mut ShuffleWriter,
    arrow_schema: &Arc<arrow_schema::Schema>,
) -> DaftResult<()> {
    let columns = RecordBatch::concat(batches)?
        .columns()
        .iter()
        .map(|c| c.as_materialized_series().to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;
    let arrow_batch = arrow_array::RecordBatch::try_new(arrow_schema.clone(), columns)
        .map_err(DaftError::ArrowRsError)?;
    writer
        .write(&arrow_batch)
        .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
    Ok(())
}

/// Streaming variant of [`write_partitions_one_shot`]: instead of receiving all partitions
/// up-front (which would hold the whole input in memory), it pulls partition `p`'s data on demand
/// via `next_partition`, so peak memory is bounded to roughly one partition.
pub async fn write_partitions_one_shot_streaming<F>(
    input_id: u32,
    shuffle_id: u64,
    shuffle_dirs: &[String],
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    num_partitions: usize,
    mut next_partition: F,
) -> DaftResult<Vec<PartitionCache>>
where
    F: FnMut(usize) -> DaftResult<MicroPartition> + Send + 'static,
{
    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);

    get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<Vec<PartitionCache>> {
            std::fs::create_dir_all(&shuffle_dir)?;
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let arrow_schema = Arc::new(schema.to_arrow()?);
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| {
                    DaftError::InternalError(format!("IPC compression init failed: {}", e))
                })?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                CountingFile::new(File::create(&file_path)?),
                arrow_schema.as_ref(),
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for idx in 0..num_partitions {
                let partition = next_partition(idx)?;
                caches.push(write_one_partition(
                    partition,
                    partition_ref_id(input_id, idx),
                    &mut writer,
                    &arrow_schema,
                    &schema,
                    &file_path,
                )?);
                // `partition` is dropped here before the next one is pulled.
            }

            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            writer
                .flush()
                .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))?;
            Ok(caches)
        })
        .await?
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field, Schema, UInt8Array};
    use daft_core::series::IntoSeries;
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::*;

    fn mp(schema: &SchemaRef, rows: usize) -> MicroPartition {
        let s = UInt8Array::from_field_and_values(
            Field::new("v", DataType::UInt8),
            (0..rows).map(|i| i as u8),
        )
        .into_series();
        let rb = RecordBatch::new_unchecked(schema.clone(), vec![s.into()], rows);
        MicroPartition::new_loaded(schema.clone(), vec![rb].into(), None)
    }

    #[tokio::test]
    async fn test_streaming_writer_matches_row_counts() -> DaftResult<()> {
        let tmp = tempfile::tempdir().unwrap();
        let dirs = vec![tmp.path().to_str().unwrap().to_string()];
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::UInt8)])).into();
        let rows_per_part = [10usize, 0, 25, 7];
        let schema_cl = schema.clone();
        let caches = write_partitions_one_shot_streaming(
            0,
            0,
            &dirs,
            schema.clone(),
            None,
            rows_per_part.len(),
            move |p| Ok(mp(&schema_cl, rows_per_part[p])),
        )
        .await?;
        assert_eq!(caches.len(), 4);
        let total: usize = caches.iter().map(|c| c.num_rows).sum();
        assert_eq!(total, 42);
        // Empty partition still produces a (zero-row) cache entry.
        assert_eq!(caches[1].num_rows, 0);
        Ok(())
    }
}
