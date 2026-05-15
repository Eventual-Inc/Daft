use std::{
    io::{BufReader, BufWriter},
    path::PathBuf,
    sync::Arc,
};

use arrow_array::ArrayRef;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;

/// Configuration for build-side spill-to-disk in hash joins.
#[derive(Clone, Debug)]
pub(crate) struct SpillConfig {
    /// If the build side exceeds this many in-memory bytes, overflowing partitions are spilled to
    /// disk. The threshold is shared across all hash partitions (`threshold / N` per partition).
    pub threshold_bytes: usize,
    /// Directories to create spill files in (round-robined per partition).
    pub spill_dirs: Vec<String>,
}

impl SpillConfig {
    pub fn new(threshold_bytes: usize, spill_dirs: Vec<String>) -> Self {
        Self {
            threshold_bytes,
            spill_dirs,
        }
    }

    /// Number of hash partitions: `max(2, ceil(threshold / 256 MiB))`, capped at 256.
    pub fn partition_count(&self) -> usize {
        const TARGET_BYTES_PER_PARTITION: usize = 256 * 1024 * 1024; // 256 MiB
        let n = (self.threshold_bytes + TARGET_BYTES_PER_PARTITION - 1)
            / TARGET_BYTES_PER_PARTITION;
        n.max(2).min(256)
    }


}

// ─── Write side (used only during build phase) ───────────────────────────────

struct SpillPartitionWriter {
    path: PathBuf,
    writer: StreamWriter<BufWriter<std::fs::File>>,
}

impl SpillPartitionWriter {
    fn open(dir: &str, arrow_schema: &arrow_schema::Schema) -> DaftResult<Self> {
        use tempfile::Builder;
        let tmp = Builder::new()
            .prefix("daft_join_spill_")
            .suffix(".ipc")
            .tempfile_in(dir)
            .map_err(|e| {
                DaftError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("failed to create spill tempfile in {dir}: {e}"),
                ))
            })?;
        // `keep()` prevents auto-deletion on drop and returns (File, PathBuf).
        // The file remains accessible at `path` until JoinSpillStore::drop removes it.
        let (file, path) = tmp.keep().map_err(|e| {
            DaftError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to keep spill tempfile: {e}"),
            ))
        })?;
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

/// Manages per-partition spill writers during the hash join build phase.
/// Consumed by `finish()` which returns an immutable `JoinSpillStore`.
pub(crate) struct JoinSpillWriter {
    partition_count: usize,
    writers: Vec<Option<SpillPartitionWriter>>,
    paths: Vec<Option<PathBuf>>,
    /// Total bytes written to disk per partition.
    pub bytes_written: Vec<usize>,
    arrow_schema: arrow_schema::Schema,
    spill_dirs: Vec<String>,
}

impl JoinSpillWriter {
    pub fn new(
        partition_count: usize,
        build_schema: &SchemaRef,
        spill_dirs: Vec<String>,
    ) -> DaftResult<Self> {
        let arrow_schema = build_schema.to_arrow()?;
        Ok(Self {
            partition_count,
            writers: (0..partition_count).map(|_| None).collect(),
            paths: (0..partition_count).map(|_| None).collect(),
            bytes_written: vec![0; partition_count],
            arrow_schema,
            spill_dirs,
        })
    }

    /// Write a `RecordBatch` to the spill file for `partition` (opens the file lazily on first
    /// write).
    pub fn write_batch(&mut self, partition: usize, batch: &RecordBatch) -> DaftResult<()> {
        self.bytes_written[partition] += batch.size_bytes();
        let writer = match &mut self.writers[partition] {
            Some(w) => w,
            None => {
                let dir = self.spill_dirs[partition % self.spill_dirs.len()].clone();
                let new_writer = SpillPartitionWriter::open(&dir, &self.arrow_schema)?;
                self.writers[partition] = Some(new_writer);
                self.writers[partition].as_mut().unwrap()
            }
        };
        writer.write(batch)
    }

    /// Whether any data has been written to disk for `partition`.
    pub fn has_spilled(&self, partition: usize) -> bool {
        self.writers[partition].is_some() || self.paths[partition].is_some()
    }

    /// Flush and close all open partition writers. Returns the immutable `JoinSpillStore` that
    /// can be shared across probe workers via `Arc`.
    pub fn finish(mut self) -> DaftResult<JoinSpillStore> {
        for p in 0..self.partition_count {
            if let Some(writer) = self.writers[p].take() {
                self.paths[p] = Some(writer.finish()?);
            }
        }
        Ok(JoinSpillStore {
            build_paths: self.paths,
        })
    }
}

// ─── Read side (shared across probe workers via Arc) ─────────────────────────

/// Immutable, thread-safe view of spilled build partitions.  Created by
/// `JoinSpillWriter::finish()`.  Files are deleted when this value is dropped.
pub(crate) struct JoinSpillStore {
    /// Spill file path per partition.  `None` means the partition stayed in memory.
    pub build_paths: Vec<Option<PathBuf>>,
}

// SAFETY: After `JoinSpillWriter::finish()` all file handles are closed; only `PathBuf`s remain.
// `PathBuf: Send + Sync`, so the derived impls hold.  We spell them out explicitly so the
// compiler does not silently infer them based on internal fields that might change later.
unsafe impl Send for JoinSpillStore {}
unsafe impl Sync for JoinSpillStore {}

impl JoinSpillStore {
    pub fn is_spilled(&self, partition: usize) -> bool {
        self.build_paths[partition].is_some()
    }

    /// Read all `RecordBatch`es written to disk for `partition`.
    pub fn read_build_partition(&self, partition: usize) -> DaftResult<Vec<RecordBatch>> {
        let path = self.build_paths[partition]
            .as_ref()
            .expect("read_build_partition called on a non-spilled partition");
        read_ipc_file(path)
    }
}

impl Drop for JoinSpillStore {
    fn drop(&mut self) {
        for path in self.build_paths.iter().flatten() {
            let _ = std::fs::remove_file(path);
        }
    }
}

// ─── Arrow IPC helpers ────────────────────────────────────────────────────────

/// Read all `RecordBatch`es from an Arrow IPC streaming file, reconstructing the schema from the
/// file header (no external schema needed).
fn read_ipc_file(path: &PathBuf) -> DaftResult<Vec<RecordBatch>> {
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
