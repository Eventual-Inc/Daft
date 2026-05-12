//! Per-output-partition append-only IPC writer for the Flight repartition path.
//!
//! Each `(shuffle_id, partition_idx)` gets exactly one file owned by a single
//! `PartitionFileWriter`. Map tasks concurrently call `write_batch` to append their
//! contribution; each returns the `(start, end)` byte range into the shared file.
//!
//! This swaps the file-boundary dimension vs `oneshot_writer`: instead of one file
//! per map task with N partitions delimited by byte ranges, we have N files per
//! actor (one per output partition), each containing M_local IPC batch messages
//! appended by the M_local map tasks that ran on this actor. Read-side win: a
//! consumer fetching partition P does a single sequential read per actor instead
//! of M_local random reads across M_local files.
//!
//! On-disk layout matches the existing `multi_partition_cache` reader contract:
//!   [ IPC schema header ]               (written once on `try_new`)
//!   [ batch ] [ batch ] [ batch ] ...   (one IPC message per map-task contribution)
//! The schema header is what `StreamWriter::try_new_with_options` emits; it's not
//! included in any byte range, and the read side re-builds it in-memory from the
//! schema (see `build_schema_header_bytes` in `server/flight_server.rs`).

use std::{
    fs::File,
    io::{self, Write},
    path::Path,
};

use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use common_error::{DaftError, DaftResult};
use daft_schema::schema::SchemaRef;
use tokio::sync::Mutex;

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

pub struct PartitionFileWriter {
    pub file_path: String,
    /// Tokio mutex (not std) so the holder can `.await` inside the critical section.
    /// In practice the critical section is just the synchronous `writer.write(&batch)`
    /// call — no awaits — but the mutex needs to be `Send` across await points in the
    /// caller's async context, and `tokio::sync::Mutex` is the standard choice for that.
    writer: Mutex<StreamWriter<CountingFile>>,
}

impl PartitionFileWriter {
    pub fn try_new(
        shuffle_dirs: &[String],
        shuffle_id: u64,
        partition_idx: usize,
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let dir_idx = partition_idx % shuffle_dirs.len();
        let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);
        if !Path::new(&shuffle_dir).exists() {
            std::fs::create_dir_all(&shuffle_dir)?;
        }
        let file_path = format!("{}/part_{}.arrow", shuffle_dir, partition_idx);
        let file = File::create(&file_path)?;
        let counting = CountingFile::new(file);
        let arrow_schema = schema.to_arrow()?;
        let writer = StreamWriter::try_new_with_options(
            counting,
            &arrow_schema,
            IpcWriteOptions::default(),
        )
        .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;
        Ok(Self {
            file_path,
            writer: Mutex::new(writer),
        })
    }

    /// Append one IPC batch message to the shared file and return its byte range.
    /// Concurrent calls serialize on the mutex; throughput is bounded by the time spent
    /// inside `writer.write(&batch)` (memcpy + syscall). Concat/encode work is the
    /// caller's responsibility and should happen outside the lock.
    pub async fn write_batch(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> DaftResult<(u64, u64)> {
        let mut writer = self.writer.lock().await;
        let before = writer.get_ref().bytes_written;
        writer
            .write(batch)
            .map_err(|e| DaftError::InternalError(format!("IPC write failed: {}", e)))?;
        let after = writer.get_ref().bytes_written;
        Ok((before, after))
    }
}
