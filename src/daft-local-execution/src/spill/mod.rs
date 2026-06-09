//! Shared spill-to-disk facility for blocking sinks (hash join, sort, grouped aggregation).
//!
//! Two write shapes are provided over a common Arrow IPC stream-file primitive:
//! - [`SpillWriter`] / [`SpillStore`]: a fixed set of `N` hash buckets, each its own file. Used by
//!   the hash join build side and by grace aggregation. Read back whole-bucket.
//! - [`SpillRunWriter`] / [`SpilledRun`] / [`SpillRunReader`]: a single sorted "run" file produced
//!   once and consumed once, read back *incrementally* (one batch at a time). Used by the external
//!   merge sort so the k-way merge only keeps one batch per run resident.
//!
//! All files are written to round-robined `spill_dirs` (typically `flight_shuffle_dirs`) and are
//! deleted when the owning handle (`SpillStore` / `SpilledRun`) is dropped.

use std::{
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::ArrayRef;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;

/// Configuration for spill-to-disk.
#[derive(Clone, Debug)]
pub(crate) struct SpillConfig {
    /// If accumulated in-memory bytes exceed this threshold, data is spilled to disk. For the hash
    /// join the threshold is shared across all hash partitions (`threshold / N` per partition).
    pub threshold_bytes: usize,
    /// Directories to create spill files in (round-robined per bucket/run).
    pub spill_dirs: Vec<String>,
}

impl SpillConfig {
    pub fn new(threshold_bytes: usize, spill_dirs: Vec<String>) -> Self {
        Self {
            threshold_bytes,
            spill_dirs,
        }
    }

    /// Number of hash partitions for the join build side: `max(2, ceil(threshold / 256 MiB))`,
    /// capped at 256.
    pub fn partition_count(&self) -> usize {
        const TARGET_BYTES_PER_PARTITION: usize = 256 * 1024 * 1024; // 256 MiB
        let n =
            (self.threshold_bytes + TARGET_BYTES_PER_PARTITION - 1) / TARGET_BYTES_PER_PARTITION;
        n.max(2).min(256)
    }
}

fn io_err(context: &str, e: impl std::fmt::Display) -> DaftError {
    DaftError::IoError(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("{context}: {e}"),
    ))
}

// ─── Low-level single-file Arrow IPC stream writer ──────────────────────────

/// One Arrow IPC stream file. The backing tempfile is `keep()`-ed so it survives this writer being
/// dropped; deletion is the responsibility of the owning [`SpillStore`] / [`SpilledRun`].
struct IpcFileWriter {
    path: PathBuf,
    writer: StreamWriter<BufWriter<std::fs::File>>,
}

impl IpcFileWriter {
    fn open(dir: &str, prefix: &str, arrow_schema: &arrow_schema::Schema) -> DaftResult<Self> {
        use tempfile::Builder;
        let tmp = Builder::new()
            .prefix(prefix)
            .suffix(".ipc")
            .tempfile_in(dir)
            .map_err(|e| io_err(&format!("failed to create spill tempfile in {dir}"), e))?;
        // `keep()` prevents auto-deletion on drop and returns (File, PathBuf).
        let (file, path) = tmp
            .keep()
            .map_err(|e| io_err("failed to keep spill tempfile", e))?;
        let writer = StreamWriter::try_new(BufWriter::new(file), arrow_schema)?;
        Ok(Self { path, writer })
    }

    fn write(&mut self, batch: &RecordBatch) -> DaftResult<()> {
        let arrow_batch: arrow_array::RecordBatch = batch.clone().try_into()?;
        self.writer.write(&arrow_batch)?;
        Ok(())
    }

    fn finish(mut self) -> DaftResult<PathBuf> {
        self.writer.finish()?;
        Ok(self.path)
    }
}

// ─── N-bucket writer (hash join build side, grace aggregation) ──────────────

/// Manages per-bucket spill writers. Consumed by `finish()` which returns an immutable
/// [`SpillStore`].
pub(crate) struct SpillWriter {
    bucket_count: usize,
    writers: Vec<Option<IpcFileWriter>>,
    paths: Vec<Option<PathBuf>>,
    /// Total bytes written to disk per bucket.
    pub bytes_written: Vec<usize>,
    arrow_schema: arrow_schema::Schema,
    spill_dirs: Vec<String>,
    file_prefix: &'static str,
}

impl SpillWriter {
    pub fn new(
        bucket_count: usize,
        schema: &SchemaRef,
        spill_dirs: Vec<String>,
        file_prefix: &'static str,
    ) -> DaftResult<Self> {
        let arrow_schema = schema.to_arrow()?;
        Ok(Self {
            bucket_count,
            writers: (0..bucket_count).map(|_| None).collect(),
            paths: (0..bucket_count).map(|_| None).collect(),
            bytes_written: vec![0; bucket_count],
            arrow_schema,
            spill_dirs,
            file_prefix,
        })
    }

    /// Write a `RecordBatch` to the spill file for `bucket` (opens the file lazily on first write).
    pub fn write_batch(&mut self, bucket: usize, batch: &RecordBatch) -> DaftResult<()> {
        self.bytes_written[bucket] += batch.size_bytes();
        let writer = match &mut self.writers[bucket] {
            Some(w) => w,
            None => {
                let dir = self.spill_dirs[bucket % self.spill_dirs.len()].clone();
                let new_writer = IpcFileWriter::open(&dir, self.file_prefix, &self.arrow_schema)?;
                self.writers[bucket] = Some(new_writer);
                self.writers[bucket].as_mut().unwrap()
            }
        };
        writer.write(batch)
    }

    /// Whether any data has been written to disk for `bucket`.
    pub fn has_spilled(&self, bucket: usize) -> bool {
        self.writers[bucket].is_some() || self.paths[bucket].is_some()
    }

    /// Flush and close all open bucket writers. Returns the immutable [`SpillStore`] that can be
    /// shared across workers via `Arc`.
    pub fn finish(mut self) -> DaftResult<SpillStore> {
        for b in 0..self.bucket_count {
            if let Some(writer) = self.writers[b].take() {
                self.paths[b] = Some(writer.finish()?);
            }
        }
        Ok(SpillStore {
            bucket_paths: self.paths,
        })
    }
}

/// Immutable, thread-safe view of spilled buckets. Created by [`SpillWriter::finish`]. Files are
/// deleted when this value is dropped.
pub(crate) struct SpillStore {
    /// Spill file path per bucket. `None` means the bucket stayed in memory.
    pub bucket_paths: Vec<Option<PathBuf>>,
}

// SAFETY: After `SpillWriter::finish()` all file handles are closed; only `PathBuf`s remain.
unsafe impl Send for SpillStore {}
unsafe impl Sync for SpillStore {}

impl SpillStore {
    pub fn is_spilled(&self, bucket: usize) -> bool {
        self.bucket_paths[bucket].is_some()
    }

    /// Read all `RecordBatch`es written to disk for `bucket`.
    pub fn read_bucket(&self, bucket: usize) -> DaftResult<Vec<RecordBatch>> {
        let path = self.bucket_paths[bucket]
            .as_ref()
            .expect("read_bucket called on a non-spilled bucket");
        read_ipc_file(path)
    }
}

impl Drop for SpillStore {
    fn drop(&mut self) {
        for path in self.bucket_paths.iter().flatten() {
            let _ = std::fs::remove_file(path);
        }
    }
}

// ─── Single sorted-run writer / handle / incremental reader (external sort) ──

/// Writes one independent run file. Tracks rows + bytes so the merge can size resident memory.
pub(crate) struct SpillRunWriter {
    writer: IpcFileWriter,
    num_rows: usize,
    num_bytes: usize,
}

impl SpillRunWriter {
    pub fn open(dir: &str, prefix: &'static str, schema: &SchemaRef) -> DaftResult<Self> {
        let arrow_schema = schema.to_arrow()?;
        Ok(Self {
            writer: IpcFileWriter::open(dir, prefix, &arrow_schema)?,
            num_rows: 0,
            num_bytes: 0,
        })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> DaftResult<()> {
        self.num_rows += batch.len();
        self.num_bytes += batch.size_bytes();
        self.writer.write(batch)
    }

    pub fn finish(self) -> DaftResult<SpilledRun> {
        let num_rows = self.num_rows;
        let num_bytes = self.num_bytes;
        let path = self.writer.finish()?;
        Ok(SpilledRun {
            path,
            num_rows,
            num_bytes,
        })
    }
}

/// Handle to one spilled run on disk. Deletes the file when dropped.
pub(crate) struct SpilledRun {
    pub path: PathBuf,
    /// Row / byte counts of the run (used for diagnostics and future multi-pass merge sizing).
    #[allow(dead_code)]
    pub num_rows: usize,
    #[allow(dead_code)]
    pub num_bytes: usize,
}

impl Drop for SpilledRun {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Incremental reader over a single run file. Yields one `RecordBatch` at a time so the merge keeps
/// only one batch per run resident.
pub(crate) struct SpillRunReader {
    reader: StreamReader<BufReader<std::fs::File>>,
    schema: SchemaRef,
}

impl SpillRunReader {
    pub fn open(path: &Path) -> DaftResult<Self> {
        let file = std::fs::File::open(path)?;
        let reader = StreamReader::try_new(BufReader::new(file), None)?;
        let schema: SchemaRef = Arc::new(reader.schema().as_ref().try_into()?);
        Ok(Self { reader, schema })
    }

    #[allow(dead_code)]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Iterator for SpillRunReader {
    type Item = DaftResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let arrow_batch = match self.reader.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e.into())),
        };
        let arrays: Vec<ArrayRef> = arrow_batch.columns().to_vec();
        Some(RecordBatch::from_arrow(self.schema.clone(), arrays))
    }
}

// ─── Arrow IPC helpers ──────────────────────────────────────────────────────

/// Read all `RecordBatch`es from an Arrow IPC streaming file, reconstructing the schema from the
/// file header (no external schema needed).
fn read_ipc_file(path: &Path) -> DaftResult<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)?;
    let reader = StreamReader::try_new(BufReader::new(file), None)?;
    let arrow_schema = reader.schema();
    let schema: SchemaRef = Arc::new(arrow_schema.as_ref().try_into()?);
    let mut batches = Vec::new();
    for result in reader {
        let arrow_batch = result?;
        let arrays: Vec<ArrayRef> = arrow_batch.columns().to_vec();
        batches.push(RecordBatch::from_arrow(schema.clone(), arrays)?);
    }
    Ok(batches)
}
