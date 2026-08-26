//! Publishing a combined map file to shared storage.
//!
//! Commit is a single atomic `rename` of a single file, so the published path is
//! either absent or a complete, self-describing map file. That is what lets two
//! attempts of the same map task (which reuse the same `task_id`, and therefore
//! the same final path) run concurrently without corrupting each other, and it is
//! why the layout needs no attempt number.

use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    process,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use common_error::{DaftError, DaftResult};

use super::{ShuffleDurability, index, shared_map_file, shared_shard_dir};

/// In-flight background `fsync`s allowed before the map side starts paying for
/// them inline. Each queued entry holds an open file descriptor, so this also
/// bounds fd usage.
const FSYNC_QUEUE_CAPACITY: usize = 64;
const FSYNC_WORKER_THREADS: usize = 4;

fn fsync_queue() -> &'static std::sync::mpsc::SyncSender<File> {
    static QUEUE: OnceLock<std::sync::mpsc::SyncSender<File>> = OnceLock::new();
    QUEUE.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::sync_channel::<File>(FSYNC_QUEUE_CAPACITY);
        let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));
        for i in 0..FSYNC_WORKER_THREADS {
            let rx = rx.clone();
            // Detached on purpose: these outlive any single query and only ever
            // block on fsync.
            std::thread::Builder::new()
                .name(format!("daft-shuffle-fsync-{}", i))
                .spawn(move || {
                    loop {
                        let file = {
                            let Ok(guard) = rx.lock() else { return };
                            match guard.recv() {
                                Ok(file) => file,
                                Err(_) => return,
                            }
                        };
                        if let Err(e) = file.sync_all() {
                            tracing::warn!("Background shuffle fsync failed: {}", e);
                        }
                    }
                })
                .expect("failed to spawn shuffle fsync thread");
        }
        tx
    })
}

/// Hand `file` to the background fsync pool, falling back to an inline `fsync`
/// when the pool is saturated. Durability is never silently skipped.
fn fsync_in_background(file: File) -> DaftResult<()> {
    match fsync_queue().try_send(file) {
        Ok(()) => Ok(()),
        Err(std::sync::mpsc::TrySendError::Full(file))
        | Err(std::sync::mpsc::TrySendError::Disconnected(file)) => {
            file.sync_all().map_err(DaftError::IoError)
        }
    }
}

fn next_temp_suffix() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// A shared-storage map file being written. Created by [`Self::begin`], published
/// by [`Self::commit`]; dropping without committing removes the temporary.
pub struct SharedMapFileCommit {
    final_path: String,
    temp_path: String,
    committed: bool,
}

impl SharedMapFileCommit {
    /// Create the temporary file and reserve its index region.
    ///
    /// Returns the handle, the open file positioned at the start of the data
    /// region, and that region's offset — which the caller must use as the base
    /// for the byte offsets it records, so that the ranges it reports are
    /// absolute file offsets.
    pub fn begin(
        shared_root: &str,
        shuffle_id: u64,
        input_id: u32,
        num_partitions: usize,
    ) -> DaftResult<(Self, File, u64)> {
        let shard_dir = shared_shard_dir(shared_root, shuffle_id, input_id);
        std::fs::create_dir_all(&shard_dir)?;

        let final_path = shared_map_file(shared_root, shuffle_id, input_id);
        let temp_path = format!(
            "{}.tmp.{}.{}",
            final_path,
            process::id(),
            next_temp_suffix()
        );

        let region_bytes = index::index_region_bytes(num_partitions);
        let mut file = File::create(&temp_path)?;
        // Reserve the region; the real offsets are written back at commit time.
        file.write_all(&vec![0u8; region_bytes])?;

        Ok((
            Self {
                final_path,
                temp_path,
                committed: false,
            },
            file,
            region_bytes as u64,
        ))
    }

    /// Fill in the index, apply `durability`, and publish the file.
    ///
    /// `offsets` holds `num_partitions + 1` absolute file offsets.
    ///
    /// Ordering differs by durability level so that publication always means at
    /// least as much as the caller asked for: [`ShuffleDurability::Sync`] fsyncs
    /// before the rename, so a visible file is a durable one, while
    /// [`ShuffleDurability::Background`] renames first and lets durability catch
    /// up, trading a crash window for the map task's critical path.
    pub fn commit(
        mut self,
        mut file: File,
        offsets: &[u64],
        durability: ShuffleDurability,
    ) -> DaftResult<()> {
        let index_bytes = index::encode(offsets)?;
        file.flush()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&index_bytes)?;
        file.flush()?;

        match durability {
            ShuffleDurability::Sync => {
                file.sync_all()?;
                drop(file);
                std::fs::rename(&self.temp_path, &self.final_path)?;
            }
            ShuffleDurability::Background => {
                std::fs::rename(&self.temp_path, &self.final_path)?;
                fsync_in_background(file)?;
            }
            ShuffleDurability::None => {
                drop(file);
                std::fs::rename(&self.temp_path, &self.final_path)?;
            }
        }

        self.committed = true;
        Ok(())
    }
}

impl Drop for SharedMapFileCommit {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn tempdir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "daft_shared_writer_test_{}_{}",
            process::id(),
            next_temp_suffix()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn commit_publishes_an_indexed_file() {
        let dir = tempdir();
        let root = dir.to_str().unwrap();
        let (commit, mut file, base) = SharedMapFileCommit::begin(root, 3, 9, 2).unwrap();
        assert_eq!(base, index::index_region_bytes(2) as u64);

        file.write_all(b"aaaabbbb").unwrap();
        commit
            .commit(file, &[base, base + 4, base + 8], ShuffleDurability::None)
            .unwrap();

        let path = shared_map_file(root, 3, 9);
        let mut published = Vec::new();
        File::open(&path)
            .unwrap()
            .read_to_end(&mut published)
            .unwrap();
        assert_eq!(index::parse_num_partitions(&published, &path).unwrap(), 2);
        assert_eq!(
            index::partition_range(&published, 0, &path).unwrap(),
            (base, base + 4)
        );
        assert_eq!(
            index::partition_range(&published, 1, &path).unwrap(),
            (base + 4, base + 8)
        );
        assert_eq!(&published[base as usize..], b"aaaabbbb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn dropping_without_commit_leaves_no_files() {
        let dir = tempdir();
        let root = dir.to_str().unwrap();
        let (commit, file, _) = SharedMapFileCommit::begin(root, 1, 2, 4).unwrap();
        let temp_path = commit.temp_path.clone();
        drop(file);
        drop(commit);
        assert!(!std::path::Path::new(&temp_path).exists());
        assert!(!std::path::Path::new(&shared_map_file(root, 1, 2)).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn concurrent_attempts_publish_one_complete_file() {
        let dir = tempdir();
        let root = dir.to_str().unwrap();
        // Two attempts of the same map task race to the same final path.
        let (c1, mut f1, base) = SharedMapFileCommit::begin(root, 5, 5, 1).unwrap();
        let (c2, mut f2, _) = SharedMapFileCommit::begin(root, 5, 5, 1).unwrap();
        f1.write_all(b"1111").unwrap();
        f2.write_all(b"2222").unwrap();
        c1.commit(f1, &[base, base + 4], ShuffleDurability::None)
            .unwrap();
        c2.commit(f2, &[base, base + 4], ShuffleDurability::None)
            .unwrap();

        let path = shared_map_file(root, 5, 5);
        let mut published = Vec::new();
        File::open(&path)
            .unwrap()
            .read_to_end(&mut published)
            .unwrap();
        // Whichever attempt won, the file is whole.
        assert_eq!(published.len(), base as usize + 4);
        assert!(&published[base as usize..] == b"1111" || &published[base as usize..] == b"2222");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
