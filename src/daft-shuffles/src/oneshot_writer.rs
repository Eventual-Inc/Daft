//! One-shot combined-file shuffle writer for the Flight backend.
//!
//! Writes all output partitions of a single map task into one IPC stream file:
//!   [ schema header ] [ partition 0 batches ] ... [ partition N-1 batches ] [ EOS ]
//! Per-partition (start, end) byte ranges let the Flight server serve a single
//! output partition without scanning neighbours.
//!
//! With [`OneShotTarget::Shared`] the same stream is preceded by an index region
//! recording those ranges (see [`crate::store::index`]) and published by atomic
//! rename, so a node that never saw the writer can resolve the ranges itself.

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

use crate::{
    shuffle_cache::{CHUNK_TARGET_BYTES, PartitionCache, partition_ref_id},
    store::{
        ShuffleDurability, create_file_under, map_file_name, shared_map_file,
        writer::SharedMapFileCommit,
    },
};

/// 4 MiB BufWriter capacity — amortizes syscall cost across multiple
/// small IPC writes per stripe. `StreamWriter::write` issues several
/// `write` calls per batch (continuation marker, metadata flatbuffer,
/// padding, then each column buffer); at sub-256-KiB stripes these
/// were going straight to the kernel as separate syscalls.
///
/// Sized to match [`CHUNK_TARGET_BYTES`] rather than the 1 MiB it used to be:
/// shared mounts reward larger writes disproportionately (measured on Lustre,
/// 1 MiB chunks sustained ~320-460 MiB/s against ~610 MiB/s for 4 MiB), and
/// node-local disks are indifferent.
const FILE_BUF_BYTES: usize = CHUNK_TARGET_BYTES;

struct CountingFile {
    inner: BufWriter<File>,
    bytes_written: u64,
    /// CRC-32 of everything written since the last `crc_reset`. Fed at the
    /// logical level (before buffering), so it tracks exactly the bytes the
    /// offsets describe.
    hasher: crc32fast::Hasher,
}

impl CountingFile {
    /// `start_offset` is where the IPC stream begins in the file, so that the
    /// byte counter — and therefore every recorded range — is an absolute file
    /// offset even when an index region precedes the stream.
    fn new_at(inner: File, start_offset: u64) -> Self {
        Self {
            inner: BufWriter::with_capacity(FILE_BUF_BYTES, inner),
            bytes_written: start_offset,
            hasher: crc32fast::Hasher::new(),
        }
    }

    fn crc_reset(&mut self) {
        self.hasher.reset();
    }

    fn crc_current(&self) -> u32 {
        self.hasher.clone().finalize()
    }

    fn into_file(self) -> DaftResult<File> {
        self.inner
            .into_inner()
            .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))
    }
}

impl Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes_written += n as u64;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

type ShuffleWriter = arrow_ipc::writer::StreamWriter<CountingFile>;

/// Where one map task's combined file goes.
#[derive(Clone, Debug)]
pub enum OneShotTarget {
    /// Node-local disk, round-robined across `shuffle_dirs`. Reachable only via
    /// the writing worker's Flight server, which holds the byte ranges in memory.
    Local { shuffle_dirs: Vec<String> },
    /// A cluster-shared mount. The file carries its own index and is published by
    /// atomic rename, so any node can read it without consulting the writer.
    Shared {
        shared_root: String,
        durability: ShuffleDurability,
    },
}

/// Write `partitions` to a single combined IPC file. Returns one
/// `PartitionCache` per output partition with the byte range to read.
///
/// `attempt` identifies this execution of the map task (see
/// [`crate::store::new_attempt_token`]); it is part of the file name for both
/// targets so that concurrent attempts of one task never touch the same file.
pub async fn write_partitions_one_shot(
    input_id: u32,
    shuffle_id: u64,
    attempt: u64,
    target: OneShotTarget,
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    partitions: Vec<MicroPartition>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitions.len();

    // IPC encode + disk write all run on a single spawn_blocking thread.
    // Previously we fanned out per-partition `tokio::spawn` calls, but at
    // N=8192 partitions per map task that was 1.6M task allocations whose
    // scheduling overhead exceeded the actual work.
    get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<Vec<PartitionCache>> {
            let (mut commit, file, base_offset, file_path) =
                open_target(&target, shuffle_id, input_id, attempt, num_partitions)?;

            let arrow_schema = Arc::new(schema.to_arrow()?);
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| {
                    DaftError::InternalError(format!("IPC compression init failed: {}", e))
                })?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                CountingFile::new_at(file, base_offset),
                arrow_schema.as_ref(),
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            // Partition boundaries and checksums double as the on-disk index for
            // the shared target: `offsets[p]..offsets[p + 1]` is partition `p`
            // (an empty partition is a zero-length range rather than a gap) and
            // `crcs[p]` covers exactly those bytes.
            let mut offsets: Vec<u64> = Vec::with_capacity(num_partitions + 1);
            let mut crcs: Vec<u32> = Vec::with_capacity(num_partitions);
            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for (idx, partition) in partitions.into_iter().enumerate() {
                offsets.push(writer.get_ref().bytes_written);
                writer.get_mut().crc_reset();
                caches.push(write_one_partition(
                    partition,
                    partition_ref_id(input_id, idx),
                    &mut writer,
                    &arrow_schema,
                    &schema,
                    &file_path,
                )?);
                crcs.push(writer.get_ref().crc_current());
            }
            // Closing bound of the last partition, taken before `finish` so the
            // EOS marker falls outside every range.
            offsets.push(writer.get_ref().bytes_written);

            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            // BufWriter::drop swallows flush errors — surface them explicitly.
            writer
                .flush()
                .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))?;

            if let Some(commit) = commit.take() {
                let durability = match &target {
                    OneShotTarget::Shared { durability, .. } => *durability,
                    OneShotTarget::Local { .. } => {
                        unreachable!("commit only exists for shared targets")
                    }
                };
                let file = writer
                    .into_inner()
                    .map_err(|e| {
                        DaftError::InternalError(format!("IPC writer into_inner failed: {}", e))
                    })?
                    .into_file()?;
                commit.commit(file, &offsets, &crcs, durability)?;
            }

            Ok(caches)
        })
        .await?
}

/// Create the destination file and report where the IPC stream starts within it.
///
/// The returned path is the one recorded in each `PartitionCache`, i.e. the path
/// readers will use — for the shared target that is the post-rename path, not the
/// temporary actually being written.
fn open_target(
    target: &OneShotTarget,
    shuffle_id: u64,
    input_id: u32,
    attempt: u64,
    num_partitions: usize,
) -> DaftResult<(Option<SharedMapFileCommit>, File, u64, String)> {
    match target {
        OneShotTarget::Local { shuffle_dirs } => {
            let dir_idx = (input_id as usize) % shuffle_dirs.len();
            let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);
            let file_path = format!("{}/{}", shuffle_dir, map_file_name(input_id, attempt));
            // Local files are written under their final name — the attempt token
            // already fences concurrent attempts — so there is no rename to make
            // durable and the directory sync state goes unused.
            let (file, _dir_sync) = create_file_under(shuffle_id, &shuffle_dir, &file_path)?;
            Ok((None, file, 0, file_path))
        }
        OneShotTarget::Shared { shared_root, .. } => {
            let (commit, file, base_offset) = SharedMapFileCommit::begin(
                shared_root,
                shuffle_id,
                input_id,
                attempt,
                num_partitions,
            )?;
            let file_path = shared_map_file(shared_root, shuffle_id, input_id, attempt);
            Ok((Some(commit), file, base_offset, file_path))
        }
    }
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
