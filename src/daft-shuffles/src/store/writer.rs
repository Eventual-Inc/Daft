//! Publishing a combined map file to shared storage.
//!
//! Each attempt writes to a temporary under its own attempt-unique final name
//! and publishes with a single atomic `rename`, so the published path is either
//! absent or a complete, self-describing map file — a reader given the path never
//! sees a half-written one. Attempt isolation itself comes from the token in the
//! file name (see [`super::shared_map_file`]); the rename is what makes a crash
//! mid-write leave nothing addressable behind.

use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    process,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use common_error::{DaftError, DaftResult};

use super::{ShuffleDurability, index, shared_map_file, shared_shard_dir};

/// Background `fsync`s allowed in flight per process before the map side starts
/// paying for them inline.
///
/// Each one is a thread parked in `fsync` for roughly the filesystem's commit
/// latency (~1 s on the reference Lustre mount, where syncs are batched into
/// transaction groups so latency stays flat as concurrency rises). The cap is
/// well above any realistic map-task completion rate for one node, and inline
/// fallback past it means durability is never silently skipped.
const MAX_BACKGROUND_FSYNCS: usize = 64;

static BACKGROUND_FSYNCS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// `fsync` the published file off the critical path.
///
/// Reopens by path rather than reusing the write handle so the writer can close
/// its handle *before* publishing: on filesystems with close-to-open semantics
/// that close is what makes the data visible to other nodes, and publication
/// must not precede it. `fsync` acts on the inode, so a fresh read-only handle
/// flushes exactly the pages the writer left dirty.
fn fsync_in_background(path: String) -> DaftResult<()> {
    let prior = BACKGROUND_FSYNCS_IN_FLIGHT.fetch_add(1, Ordering::AcqRel);
    if prior >= MAX_BACKGROUND_FSYNCS {
        BACKGROUND_FSYNCS_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        return File::open(&path)?.sync_all().map_err(DaftError::IoError);
    }

    let spawned = std::thread::Builder::new()
        .name("daft-shuffle-fsync".to_string())
        .spawn(move || {
            let result = File::open(&path).and_then(|f| f.sync_all());
            if let Err(e) = result {
                // A file that vanished was cleaned up by a finished query; anything
                // else means the durability we promised did not happen.
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!("Background shuffle fsync of {} failed: {}", path, e);
                }
            }
            BACKGROUND_FSYNCS_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        });
    if let Err(e) = spawned {
        BACKGROUND_FSYNCS_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        return Err(DaftError::IoError(e));
    }
    Ok(())
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
        attempt: u64,
        num_partitions: usize,
    ) -> DaftResult<(Self, File, u64)> {
        let shard_dir = shared_shard_dir(shared_root, shuffle_id, input_id);
        std::fs::create_dir_all(&shard_dir)?;

        let final_path = shared_map_file(shared_root, shuffle_id, input_id, attempt);
        let temp_path = format!(
            "{}.tmp.{}.{}",
            final_path,
            process::id(),
            next_temp_suffix()
        );

        let region_bytes = index::index_region_bytes(num_partitions);
        let mut file = File::create(&temp_path)?;
        // Reserve the region; the real index is written back at commit time.
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
    /// `offsets` holds `num_partitions + 1` absolute file offsets and `crcs` one
    /// CRC-32 per partition.
    ///
    /// In every mode the write handle is closed before the rename, so that on
    /// close-to-open filesystems the data is flushed to the server before any
    /// other node can learn the file's name. The modes differ only in when
    /// `fsync` happens relative to publication: [`ShuffleDurability::Sync`] before
    /// (a visible file is a durable one), [`ShuffleDurability::Background`] after
    /// and off the critical path, [`ShuffleDurability::None`] never.
    pub fn commit(
        mut self,
        mut file: File,
        offsets: &[u64],
        crcs: &[u32],
        durability: ShuffleDurability,
    ) -> DaftResult<()> {
        let index_bytes = index::encode(offsets, crcs)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&index_bytes)?;
        file.flush()?;

        if durability == ShuffleDurability::Sync {
            file.sync_all()?;
        }
        drop(file);
        std::fs::rename(&self.temp_path, &self.final_path)?;
        self.committed = true;

        if durability == ShuffleDurability::Background {
            fsync_in_background(self.final_path.clone())?;
        }
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

    fn read_all(path: &str) -> Vec<u8> {
        let mut out = Vec::new();
        File::open(path).unwrap().read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn commit_publishes_an_indexed_file() {
        let dir = tempdir();
        let root = dir.to_str().unwrap();
        let (commit, mut file, base) = SharedMapFileCommit::begin(root, 3, 9, 0x11, 2).unwrap();
        assert_eq!(base, index::index_region_bytes(2) as u64);

        file.write_all(b"aaaabbbb").unwrap();
        commit
            .commit(
                file,
                &[base, base + 4, base + 8],
                &[crc32fast::hash(b"aaaa"), crc32fast::hash(b"bbbb")],
                ShuffleDurability::None,
            )
            .unwrap();

        let path = shared_map_file(root, 3, 9, 0x11);
        let published = read_all(&path);
        assert_eq!(index::parse_num_partitions(&published, &path).unwrap(), 2);
        let e0 = index::partition_entry(&published, 0, &path).unwrap();
        let e1 = index::partition_entry(&published, 1, &path).unwrap();
        assert_eq!(
            (e0.start, e0.end, e0.crc32),
            (base, base + 4, crc32fast::hash(b"aaaa"))
        );
        assert_eq!(
            (e1.start, e1.end, e1.crc32),
            (base + 4, base + 8, crc32fast::hash(b"bbbb"))
        );
        assert_eq!(&published[base as usize..], b"aaaabbbb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn every_durability_level_publishes_a_complete_file() {
        for durability in [
            ShuffleDurability::None,
            ShuffleDurability::Background,
            ShuffleDurability::Sync,
        ] {
            let dir = tempdir();
            let root = dir.to_str().unwrap();
            let (commit, mut file, base) = SharedMapFileCommit::begin(root, 1, 1, 7, 1).unwrap();
            file.write_all(b"zzzz").unwrap();
            commit
                .commit(
                    file,
                    &[base, base + 4],
                    &[crc32fast::hash(b"zzzz")],
                    durability,
                )
                .unwrap();
            let published = read_all(&shared_map_file(root, 1, 1, 7));
            assert_eq!(&published[base as usize..], b"zzzz", "{durability:?}");
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[test]
    fn dropping_without_commit_leaves_no_files() {
        let dir = tempdir();
        let root = dir.to_str().unwrap();
        let (commit, file, _) = SharedMapFileCommit::begin(root, 1, 2, 5, 4).unwrap();
        let temp_path = commit.temp_path.clone();
        drop(file);
        drop(commit);
        assert!(!std::path::Path::new(&temp_path).exists());
        assert!(!std::path::Path::new(&shared_map_file(root, 1, 2, 5)).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn concurrent_attempts_publish_separate_complete_files() {
        let dir = tempdir();
        let root = dir.to_str().unwrap();
        // Two attempts of the same map task, alive at once.
        let (c1, mut f1, base) = SharedMapFileCommit::begin(root, 5, 5, 0xa, 1).unwrap();
        let (c2, mut f2, _) = SharedMapFileCommit::begin(root, 5, 5, 0xb, 1).unwrap();
        f1.write_all(b"1111").unwrap();
        f2.write_all(b"2222").unwrap();
        c1.commit(
            f1,
            &[base, base + 4],
            &[crc32fast::hash(b"1111")],
            ShuffleDurability::None,
        )
        .unwrap();
        c2.commit(
            f2,
            &[base, base + 4],
            &[crc32fast::hash(b"2222")],
            ShuffleDurability::None,
        )
        .unwrap();

        // Neither attempt disturbed the other; a reader addressing attempt 0xa
        // gets exactly attempt 0xa's bytes.
        let a = read_all(&shared_map_file(root, 5, 5, 0xa));
        let b = read_all(&shared_map_file(root, 5, 5, 0xb));
        assert_eq!(&a[base as usize..], b"1111");
        assert_eq!(&b[base as usize..], b"2222");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
