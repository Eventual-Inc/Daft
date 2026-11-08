//! Execution-local Arrow IPC spill files.
//!
//! The PostgreSQL bridge creates and owns the containing per-handle directory.
//! This module only creates uniquely named, owner-only files inside that
//! directory and removes every run when its Rust owner is dropped.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::Schema as ArrowSchema;
use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;

/// Cumulative metrics for an execution-local spill manager.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SpillMetrics {
    pub(crate) bytes: u64,
    pub(crate) count: u64,
}

/// Allocates and accounts immutable sorted runs below one query directory.
#[derive(Debug)]
pub(crate) struct SpillManager {
    root: PathBuf,
    next_id: AtomicU64,
    bytes: AtomicU64,
    count: AtomicU64,
}

impl SpillManager {
    pub(crate) fn new(root: PathBuf) -> DaftResult<Self> {
        let metadata = fs::metadata(&root)?;
        if !metadata.is_dir() {
            return Err(DaftError::ValueError(
                "PGDAFT_SPILL_QUERY_DIR_INVALID".to_string(),
            ));
        }
        Ok(Self {
            root,
            next_id: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            count: AtomicU64::new(0),
        })
    }

    pub(crate) fn metrics(&self) -> SpillMetrics {
        SpillMetrics {
            bytes: self.bytes.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn create_writer(self: &Arc<Self>, schema: &ArrowSchema) -> DaftResult<SpillWriter> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let path = self.root.join(format!("run-{id:016x}.arrow"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        let writer = match StreamWriter::try_new(BufWriter::new(file), schema) {
            Ok(writer) => writer,
            Err(error) => {
                let _ = fs::remove_file(&path);
                return Err(error.into());
            }
        };
        Ok(SpillWriter {
            manager: self.clone(),
            path: Some(path),
            writer: Some(writer),
        })
    }

    pub(crate) fn write_partition(
        self: &Arc<Self>,
        partition: &MicroPartition,
    ) -> DaftResult<SpillRun> {
        let schema = partition.schema().to_arrow()?;
        let mut writer = self.create_writer(&schema)?;
        for batch in partition.record_batches() {
            let arrow_batch: ArrowRecordBatch = batch.clone().try_into()?;
            writer.write(&arrow_batch)?;
        }
        writer.finish()
    }
}

/// A not-yet-finalized spill file. Drop removes partial output.
pub(crate) struct SpillWriter {
    manager: Arc<SpillManager>,
    path: Option<PathBuf>,
    writer: Option<StreamWriter<BufWriter<File>>>,
}

impl SpillWriter {
    pub(crate) fn write(&mut self, batch: &ArrowRecordBatch) -> DaftResult<()> {
        self.writer
            .as_mut()
            .expect("spill writer must be live")
            .write(batch)?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> DaftResult<SpillRun> {
        let mut writer = self.writer.take().expect("spill writer must be live");
        writer.finish()?;
        drop(writer);

        let size = fs::metadata(self.path.as_ref().expect("spill path must be live"))?.len();
        let path = self.path.take().expect("spill path must be live");
        self.manager.bytes.fetch_add(size, Ordering::Relaxed);
        self.manager.count.fetch_add(1, Ordering::Relaxed);
        Ok(SpillRun { path: Some(path) })
    }
}

impl Drop for SpillWriter {
    fn drop(&mut self) {
        self.writer.take();
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

/// An immutable spill run. Drop deletes the file.
pub(crate) struct SpillRun {
    path: Option<PathBuf>,
}

impl SpillRun {
    pub(crate) fn open(self) -> DaftResult<SpillReader> {
        let path = self.path.as_ref().expect("spill run must be live");
        let file = File::open(path)?;
        let reader = StreamReader::try_new(BufReader::new(file), None)?;
        Ok(SpillReader { reader, _run: self })
    }
}

impl Drop for SpillRun {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

/// Streaming reader that keeps its run alive until the reader is dropped.
pub(crate) struct SpillReader {
    reader: StreamReader<BufReader<File>>,
    _run: SpillRun,
}

impl SpillReader {
    pub(crate) fn schema(&self) -> Arc<ArrowSchema> {
        self.reader.schema()
    }

    pub(crate) fn next_batch(&mut self) -> DaftResult<Option<ArrowRecordBatch>> {
        self.reader.next().transpose().map_err(Into::into)
    }
}
