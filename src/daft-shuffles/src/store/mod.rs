//! Shared-disk shuffle storage: placement policy, path layout, and the on-disk
//! index that lets a node other than the writer locate one output partition
//! inside a combined map file.
//!
//! The Flight backend's default placement keeps map output on node-local disk and
//! serves it over gRPC, so a partition is only reachable while its writer is alive.
//! With a cluster-shared POSIX mount (Lustre, NFS, FSx, ...) the same combined file
//! can instead be written once to shared storage, which makes it readable by any
//! node — both as a faster route than proxying through the writer, and as the
//! recovery route when the writer is gone.
//!
//! What shared placement has to add over the local path is *addressability*. The
//! local path keeps each map file's per-partition byte ranges in the writer
//! process's memory ([`crate::shuffle_cache::PartitionCache`]), which a peer cannot
//! see. So the shared layout writes those ranges into the file itself, as a
//! fixed-size index region ahead of the IPC stream. See [`index`] for the format.

pub mod index;
pub mod reader;
pub mod writer;

use common_error::{DaftError, DaftResult};

/// Number of subdirectories map files are sharded across.
///
/// Shared filesystems commonly serialize metadata updates per directory: on the
/// Lustre mount this was developed against, concurrent creates in one directory
/// cap out around 1.1k/s but reach 4.2k/s when spread across per-writer
/// subdirectories. At 10k map tasks that difference is seconds of pure create
/// latency, so shard rather than pay it.
pub const SHARD_COUNT: u32 = 256;

/// Where a shuffle's map output is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ShufflePlacement {
    /// Write to node-local `flight_shuffle_dirs`; serve over gRPC only.
    #[default]
    LocalOnly,
    /// Write to the shared mount; readable over gRPC or directly by any node.
    SharedOnly,
}

impl ShufflePlacement {
    pub fn parse(s: Option<&str>) -> DaftResult<Self> {
        match s {
            None | Some("") | Some("local_only") => Ok(Self::LocalOnly),
            Some("shared_only") => Ok(Self::SharedOnly),
            Some(other) => Err(DaftError::ValueError(format!(
                "Unsupported flight_shuffle_placement: {}, expected 'local_only' or 'shared_only'",
                other
            ))),
        }
    }

    pub fn is_shared(self) -> bool {
        matches!(self, Self::SharedOnly)
    }
}

/// How hard the map side works to make a shared-disk write survive losing its writer.
///
/// This exists because `fsync` on a shared mount can be brutally expensive and,
/// critically, *size-independent*: on the reference Lustre deployment a single
/// `fsync` costs ~1s whether the file is 0 B or 64 MiB, and only ~25 of them
/// complete per second even at 32-way concurrency. Paying that on the map task's
/// critical path cut write throughput from ~610 MiB/s to 140-340 MiB/s.
///
/// The way out is that *visibility* and *durability* are separate problems.
/// Visibility to another node is already guaranteed without `fsync` by the shared
/// filesystem's close-to-open coherency, and that is all a reduce task needs while
/// the writer is alive. `fsync` only buys the case where the writer node dies with
/// data still in its page cache — rare, and worth deferring off the critical path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ShuffleDurability {
    /// Never `fsync`. Fastest; a shared copy may be lost if its writer node dies.
    None,
    /// Return as soon as the file is visible, then `fsync` in the background.
    #[default]
    Background,
    /// `fsync` before reporting the map output. Slowest, strongest.
    Sync,
}

impl ShuffleDurability {
    pub fn parse(s: Option<&str>) -> DaftResult<Self> {
        match s {
            None | Some("") | Some("background") => Ok(Self::Background),
            Some("none") => Ok(Self::None),
            Some("sync") => Ok(Self::Sync),
            Some(other) => Err(DaftError::ValueError(format!(
                "Unsupported flight_shuffle_shared_durability: {}, expected 'none', 'background', or 'sync'",
                other
            ))),
        }
    }
}

/// Which transport a reduce task uses to fetch a map output it does not hold locally.
///
/// Unlike placement, this is a per-worker decision: the reader is the only party
/// that knows how its own RPC and shared-mount paths are performing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ShuffleReadSource {
    /// Read directly from the shared mount when the data is there, else RPC.
    #[default]
    Auto,
    /// Always RPC to the writing worker (falls back to shared on failure).
    Rpc,
    /// Always read the shared mount (no RPC fallback; fails if not shared).
    Shared,
}

impl ShuffleReadSource {
    pub fn parse(s: Option<&str>) -> DaftResult<Self> {
        match s {
            None | Some("") | Some("auto") => Ok(Self::Auto),
            Some("rpc") => Ok(Self::Rpc),
            Some("shared") => Ok(Self::Shared),
            Some(other) => Err(DaftError::ValueError(format!(
                "Unsupported flight_shuffle_read_source: {}, expected 'auto', 'rpc', or 'shared'",
                other
            ))),
        }
    }
}

/// Root directory holding every map file of one shuffle.
pub fn shared_shuffle_dir(shared_root: &str, shuffle_id: u64) -> String {
    format!(
        "{}/daft_shuffle/{}",
        shared_root.trim_end_matches('/'),
        shuffle_id
    )
}

/// Directory holding one shard of a shuffle's map files.
pub fn shared_shard_dir(shared_root: &str, shuffle_id: u64, input_id: u32) -> String {
    format!(
        "{}/shard_{}",
        shared_shuffle_dir(shared_root, shuffle_id),
        input_id % SHARD_COUNT
    )
}

/// Committed combined map file for one map task.
///
/// No attempt number appears here on purpose. A retried map task reuses its
/// `task_id`, so two attempts can target this same path; they are kept from
/// corrupting each other by writing to distinct temporaries and committing with
/// an atomic `rename`, which makes the final path either absent or a complete
/// file, never a half-written one. A reader that already has the file open keeps
/// reading the attempt it opened.
pub fn shared_map_file(shared_root: &str, shuffle_id: u64, input_id: u32) -> String {
    format!(
        "{}/map_{}.arrow",
        shared_shard_dir(shared_root, shuffle_id, input_id),
        input_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_parses_and_defaults_to_local() {
        assert_eq!(
            ShufflePlacement::parse(None).unwrap(),
            ShufflePlacement::LocalOnly
        );
        assert_eq!(
            ShufflePlacement::parse(Some("shared_only")).unwrap(),
            ShufflePlacement::SharedOnly
        );
        assert!(ShufflePlacement::parse(Some("both")).is_err());
    }

    #[test]
    fn durability_defaults_to_background() {
        assert_eq!(
            ShuffleDurability::parse(None).unwrap(),
            ShuffleDurability::Background
        );
        assert_eq!(
            ShuffleDurability::parse(Some("sync")).unwrap(),
            ShuffleDurability::Sync
        );
        assert!(ShuffleDurability::parse(Some("fsync")).is_err());
    }

    #[test]
    fn read_source_defaults_to_auto() {
        assert_eq!(
            ShuffleReadSource::parse(None).unwrap(),
            ShuffleReadSource::Auto
        );
        assert_eq!(
            ShuffleReadSource::parse(Some("shared")).unwrap(),
            ShuffleReadSource::Shared
        );
        assert!(ShuffleReadSource::parse(Some("hedge")).is_err());
    }

    #[test]
    fn map_files_shard_by_input_id() {
        let a = shared_map_file("/mnt/shared", 7, 1);
        let b = shared_map_file("/mnt/shared/", 7, 257);
        assert_eq!(a, "/mnt/shared/daft_shuffle/7/shard_1/map_1.arrow");
        // 257 % 256 == 1, so it lands in the same shard as input 1.
        assert_eq!(b, "/mnt/shared/daft_shuffle/7/shard_1/map_257.arrow");
        assert_eq!(
            shared_shuffle_dir("/mnt/shared", 7),
            "/mnt/shared/daft_shuffle/7"
        );
    }
}
