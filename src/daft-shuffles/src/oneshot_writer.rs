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
            for (idx, parts) in partitions_per_output.into_iter().enumerate() {
                caches.push(write_one_partition(
                    parts,
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

/// Concat one output partition's MicroPartitions and write to `writer`,
/// coalescing small Daft record batches up to `CHUNK_TARGET_BYTES` per IPC
/// message. Large batches pass through unsplit.
fn write_one_partition(
    parts: Vec<MicroPartition>,
    ref_id: u64,
    writer: &mut ShuffleWriter,
    arrow_schema: &Arc<arrow_schema::Schema>,
    schema: &SchemaRef,
    file_path: &str,
) -> DaftResult<PartitionCache> {
    let num_rows: usize = parts.iter().map(|p| p.len()).sum();
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
    let combined = MicroPartition::concat(parts)?;
    let batches = combined.record_batches();

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
