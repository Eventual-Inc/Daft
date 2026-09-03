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

/// Committed combined map file for one *attempt* of one map task.
///
/// The attempt token is part of the name because a retried map task keeps its
/// `task_id` (and so its `input_id`) while the attempt it replaces may still be
/// running: Ray reports a worker as unavailable on a transient error, the
/// scheduler re-dispatches, and the original keeps executing on an actor that is
/// alive and may even be the same process. Two live attempts of one task are
/// therefore a normal event, not a corner case.
///
/// Without the token they would share a path. Each would commit a complete file,
/// but reducers that opened before and after the second rename would see
/// different attempts — and attempts are not byte-identical or even row-identical
/// (random repartitioning assigns rows by arrival position, and upstream
/// operators may be nondeterministic). Rows could then land in one partition
/// under attempt A and another under attempt B, and be read twice or not at all.
///
/// With the token, the coordinator hands reducers exactly one attempt per input
/// and every reader opens that attempt's file. A losing attempt's file is simply
/// never addressed and is removed with the shuffle directory.
pub fn shared_map_file(shared_root: &str, shuffle_id: u64, input_id: u32, attempt: u64) -> String {
    format!(
        "{}/{}",
        shared_shard_dir(shared_root, shuffle_id, input_id),
        map_file_name(input_id, attempt)
    )
}

/// File name of one attempt's combined map file, shared by the local and shared
/// layouts so the same fencing applies to both.
pub fn map_file_name(input_id: u32, attempt: u64) -> String {
    format!("map_{}_{:016x}.arrow", input_id, attempt)
}

/// Fresh token identifying one attempt of one map task.
///
/// Random rather than a counter because attempts of the same task can run in
/// different processes on different nodes with no shared sequence between them;
/// 64 random bits make an accidental collision between two attempts of the same
/// input negligible.
pub fn new_attempt_token() -> u64 {
    rand::random::<u64>()
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
    fn map_files_shard_by_input_id_and_name_the_attempt() {
        let a = shared_map_file("/mnt/shared", 7, 1, 0xabc);
        let b = shared_map_file("/mnt/shared/", 7, 257, 0xabc);
        assert_eq!(
            a,
            "/mnt/shared/daft_shuffle/7/shard_1/map_1_0000000000000abc.arrow"
        );
        // 257 % 256 == 1, so it lands in the same shard as input 1.
        assert_eq!(
            b,
            "/mnt/shared/daft_shuffle/7/shard_1/map_257_0000000000000abc.arrow"
        );
        // Two attempts of one input never share a path.
        assert_ne!(a, shared_map_file("/mnt/shared", 7, 1, 0xabd));
        assert_eq!(
            shared_shuffle_dir("/mnt/shared", 7),
            "/mnt/shared/daft_shuffle/7"
        );
    }

    #[test]
    fn attempt_tokens_are_distinct() {
        let a = new_attempt_token();
        let b = new_attempt_token();
        assert_ne!(a, b);
    }
}
