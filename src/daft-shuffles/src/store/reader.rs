//! Reading one output partition straight off the shared mount.
//!
//! This is the counterpart to the Flight `do_get` path: same combined files, same
//! byte ranges, same raw-IPC forwarding — but resolved locally from the file's own
//! index instead of from the writer's in-memory
//! [`crate::shuffle_cache::PartitionCache`]. That independence from the writer
//! process is the whole point: it removes the proxy hop while the writer is alive,
//! and it keeps the data readable once the writer is gone.
//!
//! Every range read is verified before it is trusted: the parser must consume
//! exactly the indexed byte count, and the bytes must match the indexed CRC-32.
//! A file that is short, holed, or otherwise not what the writer committed is
//! reported as an error rather than decoded into plausible-looking rows.

use std::{
    collections::HashMap,
    io::SeekFrom,
    sync::{Arc, LazyLock, Mutex},
};

use arrow_flight::{FlightData, SchemaAsIpc, decode::FlightRecordBatchStream};
use arrow_ipc::writer::IpcWriteOptions;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::{index, shared_map_file, verify::CheckedRange};
use crate::client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream;

/// One map input a reducer must gather: which task wrote it, and which attempt
/// of that task the coordinator selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MapInput {
    pub input_id: u32,
    pub attempt: u64,
}

/// Process-wide ceiling on map files being read off the shared mount at once.
///
/// Each reduce task already limits its own fan-out
/// (`flight_shuffle_shared_read_concurrency`), but a worker runs several reduce
/// tasks at once and can host more than one shuffle-read source, so the per-task
/// budgets multiply. This is the backstop that keeps that product from becoming
/// the process's open-file count: set well above any sensible per-task budget, so
/// it never shapes normal execution, and low enough that a large cluster cannot
/// push a node into fd exhaustion or bury the mount's metadata server.
const MAX_CONCURRENT_SHARED_READS: usize = 512;

static SHARED_READ_SLOTS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(MAX_CONCURRENT_SHARED_READS));

/// How many output partitions each shuffle turned out to have, learned from the
/// first map file of that shuffle this process read.
///
/// The count is a property of the *shuffle*, not of a file: every map task of one
/// shuffle writes the same number of output partitions, so one file's header
/// answers the question for all of them. That is what lets every subsequent read
/// ask for exactly the index that is there instead of speculating.
///
/// Dropped with the rest of the shuffle's memoized state (see
/// [`super::forget_shuffle`]). A stale entry costs at most one extra read, not
/// correctness — the count is re-derived from every file's own header.
static PARTITION_COUNTS: LazyLock<Mutex<HashMap<u64, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn lock_partition_counts() -> std::sync::MutexGuard<'static, HashMap<u64, usize>> {
    PARTITION_COUNTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn remembered_partition_count(shuffle_id: u64) -> Option<usize> {
    lock_partition_counts().get(&shuffle_id).copied()
}

pub(super) fn forget_partition_count(shuffle_id: u64) {
    lock_partition_counts().remove(&shuffle_id);
    INDEX_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .forget(shuffle_id);
}

/// Index regions this process has already read, keyed by the shuffle that owns
/// them and then by file path.
///
/// A reduce task reads one partition out of every map file, and a worker runs
/// many reduce tasks, so the same handful of index regions are re-read once per
/// (file, task) pair — 65536 times for a 16-file, 4096-partition shuffle, of
/// which 65520 are re-reads of bytes this process already had. Map files are
/// immutable once published and their paths carry an attempt token, so a cached
/// region can never go stale; only its owning shuffle disappearing makes it
/// useless, which is what [`super::forget_shuffle`] is for.
///
/// Regions get large at high partition counts (48 KiB at 4096 outputs), so the
/// cache is capped by bytes rather than entries.
static INDEX_CACHE: LazyLock<Mutex<IndexCache>> =
    LazyLock::new(|| Mutex::new(IndexCache::default()));

/// Bytes of index region held per process.
///
/// Sized to hold a whole shuffle's indexes at the shapes where the cache matters
/// most — 4096 map files at 4096 outputs is 192 MiB, so this holds a third of the
/// worst realistic case and all of the common ones — while staying small next to
/// the data a worker is already buffering.
const MAX_CACHED_INDEX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct IndexCache {
    by_shuffle: HashMap<u64, HashMap<String, Arc<[u8]>>>,
    bytes: usize,
}

impl IndexCache {
    fn get(&self, shuffle_id: u64, path: &str) -> Option<Arc<[u8]>> {
        self.by_shuffle.get(&shuffle_id)?.get(path).cloned()
    }

    fn insert(&mut self, shuffle_id: u64, path: &str, region: &Arc<[u8]>) {
        // Past the cap, keep what is already here and decline the newcomer rather
        // than evicting to make room. Every reduce task reads the same set of map
        // files, so with more files than budget any eviction policy is thrashing
        // against a uniform access pattern: a stable prefix that always hits beats
        // a churning working set that never does. Entries are released a whole
        // shuffle at a time (`forget`), which is when the budget actually frees up.
        if self.bytes + region.len() > MAX_CACHED_INDEX_BYTES {
            return;
        }
        if self
            .by_shuffle
            .entry(shuffle_id)
            .or_default()
            .insert(path.to_string(), region.clone())
            .is_none()
        {
            self.bytes += region.len();
        }
    }

    fn forget(&mut self, shuffle_id: u64) {
        if let Some(paths) = self.by_shuffle.remove(&shuffle_id) {
            self.bytes = self
                .bytes
                .saturating_sub(paths.values().map(|r| r.len()).sum::<usize>());
        }
    }
}

/// Read the index region of an already-open map file, and report how many
/// partitions it turned out to describe.
///
/// `expected` is the shuffle's partition count if this process has already seen
/// it. With it, the read is sized to the index that is actually there; without
/// it, one speculative [`index::PROBE_BYTES`] read covers any realistic shuffle
/// in a single round trip.
///
/// The hint is checked, never trusted: the count is always re-read from this
/// file's own header, and a hint that undershoots simply costs the second read
/// that an uncovered probe would have cost anyway.
pub(super) async fn read_index_region(
    file: &mut File,
    path: &str,
    expected: Option<usize>,
) -> DaftResult<(Vec<u8>, usize)> {
    let first_read = expected.map_or(index::PROBE_BYTES, index::index_region_bytes);
    let mut probe = vec![0u8; first_read];
    let mut filled = 0;
    while filled < probe.len() {
        let n = file.read(&mut probe[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    probe.truncate(filled);

    let num_partitions = index::parse_num_partitions(&probe, path)?;
    let needed = index::index_region_bytes(num_partitions);
    if probe.len() >= needed {
        probe.truncate(needed);
        return Ok((probe, num_partitions));
    }

    probe.resize(needed, 0);
    file.read_exact(&mut probe[filled..]).await?;
    Ok((probe, num_partitions))
}

/// Stream the raw IPC messages of one output partition out of one map file,
/// verifying the range against the index before the stream ends.
fn read_one_map_file(
    shuffle_id: u64,
    path: String,
    partition_idx: usize,
) -> BoxStream<'static, DaftResult<FlightData>> {
    Box::pin(async_stream::try_stream! {
        // Held for the whole file — open, index, and data — because the file
        // handle is what the cap is about. Acquired before the open so a caller
        // waits here rather than in the kernel's descriptor table.
        let _slot = SHARED_READ_SLOTS.acquire().await.map_err(|e| {
            DaftError::InternalError(format!("shared read semaphore closed: {}", e))
        })?;

        // TODO: recompute from lineage instead of failing. A missing file here
        // means the selected map attempt died before its commit rename landed, so
        // there is no copy anywhere and the only correct recovery is to re-run
        // that map task. Flotilla has no lineage-recompute path today, so the
        // query fails.
        let mut file = File::open(&path).await.map_err(|e| {
            DaftError::External(
                format!("Failed to open shared shuffle map file {}: {}", path, e).into(),
            )
        })?;
        let cached = INDEX_CACHE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(shuffle_id, &path);
        let region = match cached {
            Some(region) => region,
            None => {
                let expected = remembered_partition_count(shuffle_id);
                let (region, num_partitions) =
                    read_index_region(&mut file, &path, expected).await?;
                if expected != Some(num_partitions) {
                    lock_partition_counts().insert(shuffle_id, num_partitions);
                }
                let region: Arc<[u8]> = region.into();
                INDEX_CACHE
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(shuffle_id, &path, &region);
                region
            }
        };
        let entry = index::partition_entry(&region, partition_idx, &path)?;

        // An empty output partition contributes no IPC messages at all.
        if entry.is_empty() {
            return;
        }

        file.seek(SeekFrom::Start(entry.start)).await.map_err(DaftError::IoError)?;
        let mut range = CheckedRange::new(
            file,
            entry.len(),
            Some(entry.crc32),
            format!("shuffle map file {} partition {}", path, partition_idx),
        );

        while let Some(message) = range.next().await? {
            yield message;
        }
        range.finish()?;
    })
}

/// Read output partition `partition_idx` of `shuffle_id` from the shared mount,
/// gathering the contributions of every listed map input.
///
/// Map files are read unordered and concurrently: a reduce task's inputs are
/// independent and the shuffle is already an unordered exchange, so there is
/// nothing to gain from reading them in sequence and a great deal of latency to
/// lose. The batches of all files are decoded as one logical IPC stream, which
/// costs a single schema decode rather than one per file. Interleaving messages
/// from different files is sound because the stream carries only record batches:
/// Daft has no dictionary-encoded type, so there is no per-file dictionary state
/// for the decoder to confuse.
pub fn read_partition_stream(
    shared_root: &str,
    shuffle_id: u64,
    inputs: &[MapInput],
    partition_idx: u32,
    schema: SchemaRef,
    concurrency: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let paths = inputs
        .iter()
        .map(|input| shared_map_file(shared_root, shuffle_id, input.input_id, input.attempt))
        .collect::<Vec<_>>();

    let arrow_schema = schema.to_arrow()?;
    let flight_schema: FlightData =
        SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default()).into();

    let partition_idx = partition_idx as usize;
    let data = futures::stream::iter(paths)
        .flat_map_unordered(Some(concurrency.max(1)), move |path| {
            read_one_map_file(shuffle_id, path, partition_idx)
        })
        .map(|item| item.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e))));

    let flight_data = futures::stream::once(async move { Ok(flight_schema) }).chain(data);
    let arrow_stream = FlightRecordBatchStream::new_from_flight_data(flight_data);
    Ok(FlightRecordBatchStreamToDaftRecordBatchStream::new(arrow_stream, schema).boxed())
}

#[cfg(test)]
pub(super) mod tests {
    use std::{
        io::{Read, Seek, Write},
        sync::Arc,
    };

    use common_error::DaftResult;
    use daft_micropartition::MicroPartition;
    use daft_schema::{
        dtype::DataType,
        field::Field,
        schema::{Schema, SchemaRef},
    };
    use daft_writers::test::make_dummy_mp;
    use futures::TryStreamExt;

    use super::*;
    use crate::{
        oneshot_writer::{OneShotTarget, write_partitions_one_shot},
        store::ShuffleDurability,
    };

    pub(in crate::store) fn dummy_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ints", DataType::UInt8)]))
    }

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "daft_shared_roundtrip_{}_{}_{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `make_dummy_mp(n)` yields `n` rows holding `0..n` truncated to u8.
    fn partitions(schema: &SchemaRef, first: usize) -> Vec<MicroPartition> {
        vec![
            make_dummy_mp(first),
            MicroPartition::empty(Some(schema.clone())),
            make_dummy_mp(300),
        ]
    }

    async fn read_rows(
        root: &str,
        shuffle_id: u64,
        inputs: &[MapInput],
        partition_idx: u32,
        schema: SchemaRef,
    ) -> DaftResult<Vec<u8>> {
        let batches: Vec<RecordBatch> =
            read_partition_stream(root, shuffle_id, inputs, partition_idx, schema, 4)?
                .try_collect()
                .await?;
        let mut out = Vec::new();
        for batch in batches {
            let series = batch.get_column(0);
            let arr = series.u8().unwrap();
            for i in 0..arr.len() {
                out.push(arr.get(i).unwrap());
            }
        }
        Ok(out)
    }

    async fn write_shared(
        root: &str,
        shuffle_id: u64,
        input: MapInput,
        schema: SchemaRef,
        compression: Option<arrow_ipc::CompressionType>,
        first_partition_rows: usize,
    ) -> DaftResult<()> {
        write_partitions_one_shot(
            input.input_id,
            shuffle_id,
            input.attempt,
            OneShotTarget::Shared {
                shared_root: root.to_string(),
                durability: ShuffleDurability::None,
            },
            schema.clone(),
            compression,
            partitions(&schema, first_partition_rows),
        )
        .await?;
        Ok(())
    }

    fn expected(n: usize) -> Vec<u8> {
        (0..n).map(|i| i as u8).collect()
    }

    #[tokio::test]
    async fn shared_round_trip_preserves_each_partition() -> DaftResult<()> {
        let dir = tempdir("basic");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 7,
            attempt: 0x77,
        };
        write_shared(root, 42, input, schema.clone(), None, 50).await?;

        assert_eq!(
            read_rows(root, 42, &[input], 0, schema.clone()).await?,
            expected(50)
        );
        // The empty partition is a zero-length range, not a gap.
        assert!(
            read_rows(root, 42, &[input], 1, schema.clone())
                .await?
                .is_empty()
        );
        assert_eq!(
            read_rows(root, 42, &[input], 2, schema.clone()).await?,
            expected(300)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn shared_round_trip_survives_ipc_compression() -> DaftResult<()> {
        let dir = tempdir("lz4");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 0,
            attempt: 1,
        };
        write_shared(
            root,
            1,
            input,
            schema.clone(),
            Some(arrow_ipc::CompressionType::LZ4_FRAME),
            50,
        )
        .await?;

        assert_eq!(
            read_rows(root, 1, &[input], 2, schema.clone()).await?,
            expected(300)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn one_partition_gathers_every_map_input() -> DaftResult<()> {
        let dir = tempdir("fanin");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        // Three map tasks, sharded across different subdirectories.
        let inputs = [1u32, 2, 257].map(|input_id| MapInput {
            input_id,
            attempt: 9,
        });
        for input in inputs {
            write_shared(root, 9, input, schema.clone(), None, 50).await?;
        }

        let mut rows = read_rows(root, 9, &inputs, 0, schema.clone()).await?;
        rows.sort_unstable();
        let mut want = expected(50)
            .into_iter()
            .cycle()
            .take(150)
            .collect::<Vec<_>>();
        want.sort_unstable();
        assert_eq!(rows, want);

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// Two attempts of one map task, with *different* contents (as they would be
    /// under random repartitioning), both committed. The reader must return only
    /// the attempt it was asked for — never the other, never a mix.
    #[tokio::test]
    async fn reader_addresses_exactly_the_selected_attempt() -> DaftResult<()> {
        let dir = tempdir("attempts");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let losing = MapInput {
            input_id: 3,
            attempt: 0xaaaa,
        };
        let selected = MapInput {
            input_id: 3,
            attempt: 0xbbbb,
        };
        write_shared(root, 5, losing, schema.clone(), None, 40).await?;
        write_shared(root, 5, selected, schema.clone(), None, 60).await?;

        assert_eq!(
            read_rows(root, 5, &[selected], 0, schema.clone()).await?,
            expected(60)
        );
        assert_eq!(
            read_rows(root, 5, &[losing], 0, schema.clone()).await?,
            expected(40)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// The partition count is a property of the shuffle, so after one file every
    /// other file's index can be read at exactly its size instead of speculated
    /// at. What must not change is the answer.
    #[tokio::test]
    async fn a_partition_count_hint_only_changes_how_much_is_read() -> DaftResult<()> {
        let dir = tempdir("hint");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let (shuffle_id, input_id, attempt) = (31u64, 2u32, 0xa11ce_u64);
        write_partitions_one_shot(
            input_id,
            shuffle_id,
            attempt,
            OneShotTarget::Shared {
                shared_root: root.to_string(),
                durability: ShuffleDurability::None,
            },
            schema.clone(),
            None,
            partitions(&schema, 40),
        )
        .await?;
        let path = shared_map_file(root, shuffle_id, input_id, attempt);

        let mut file = File::open(&path).await?;
        let (unhinted, n) = read_index_region(&mut file, &path, None).await?;
        assert_eq!(n, 3);

        // Exact, over- and under-estimates all resolve to the same region: the
        // count is re-read from the file's own header either way, and a hint that
        // undershoots just pays for the second read.
        for hint in [Some(3), Some(64), Some(1)] {
            let mut file = File::open(&path).await?;
            let (hinted, hinted_n) = read_index_region(&mut file, &path, hint).await?;
            assert_eq!(hinted_n, 3, "hint {hint:?}");
            assert_eq!(hinted, unhinted, "hint {hint:?}");
            assert_eq!(
                index::partition_entry(&hinted, 1, &path)?,
                index::partition_entry(&unhinted, 1, &path)?,
            );
        }

        // The exact hint reads exactly the index and nothing else; the default
        // probe reads two orders of magnitude more.
        assert_eq!(unhinted.len(), index::index_region_bytes(3));
        assert!(index::index_region_bytes(3) < index::PROBE_BYTES / 100);

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// The cache is a byte-bounded map, not an unbounded one: index regions reach
    /// 48 KiB at 4096 output partitions, so a worker that read every shuffle it
    /// ever touched would hold hundreds of megabytes of them.
    #[test]
    fn index_cache_is_bounded_and_forgettable() {
        let mut cache = IndexCache::default();
        let small: Arc<[u8]> = vec![1u8; 1024].into();

        cache.insert(1, "/a", &small);
        cache.insert(1, "/b", &small);
        cache.insert(2, "/c", &small);
        assert_eq!(cache.bytes, 3 * 1024);
        assert!(cache.get(1, "/a").is_some());
        assert!(cache.get(2, "/c").is_some());
        assert!(
            cache.get(2, "/a").is_none(),
            "shuffles must not see each other"
        );

        // Re-inserting the same path is not double-counted.
        cache.insert(1, "/a", &small);
        assert_eq!(cache.bytes, 3 * 1024);

        // A shuffle going away takes its bytes with it.
        cache.forget(1);
        assert_eq!(cache.bytes, 1024);
        assert!(cache.get(1, "/a").is_none());

        // Past the cap the newcomer is declined; what is already cached stays,
        // because a stable prefix that always hits beats a churning set that never
        // does when every reader wants the same files.
        let over: Arc<[u8]> = vec![0u8; MAX_CACHED_INDEX_BYTES].into();
        cache.insert(3, "/big", &over);
        assert!(cache.get(3, "/big").is_none(), "it does not fit");
        assert!(
            cache.get(2, "/c").is_some(),
            "and must not have evicted this"
        );
        assert_eq!(cache.bytes, 1024);
    }

    /// Every partition of a file after the first is served from the cached region.
    /// The risk that buys is serving the *wrong* partition's entry out of it, so
    /// check that each one still resolves to its own rows.
    #[tokio::test]
    async fn cached_index_regions_still_resolve_each_partition() -> DaftResult<()> {
        let dir = tempdir("index_cache");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let (shuffle_id, input) = (
            0xCAC4E_u64,
            MapInput {
                input_id: 6,
                attempt: 0x6,
            },
        );
        write_shared(root, shuffle_id, input, schema.clone(), None, 40).await?;
        super::super::forget_shuffle(shuffle_id);

        // First read populates the cache; the next two must come out of it.
        assert_eq!(
            read_rows(root, shuffle_id, &[input], 0, schema.clone()).await?,
            expected(40)
        );
        assert!(
            INDEX_CACHE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(
                    shuffle_id,
                    &shared_map_file(root, shuffle_id, input.input_id, input.attempt)
                )
                .is_some(),
            "the first read should have cached the region"
        );
        // Partition 1 is empty and partition 2 has 300 rows; both are resolved from
        // the same cached bytes as partition 0 was.
        assert!(
            read_rows(root, shuffle_id, &[input], 1, schema.clone())
                .await?
                .is_empty()
        );
        assert_eq!(
            read_rows(root, shuffle_id, &[input], 2, schema.clone()).await?,
            expected(300)
        );

        super::super::forget_shuffle(shuffle_id);
        assert!(
            INDEX_CACHE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(
                    shuffle_id,
                    &shared_map_file(root, shuffle_id, input.input_id, input.attempt)
                )
                .is_none()
        );

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A count left over from a shuffle that no longer exists must not outlive it,
    /// or a long-lived worker accumulates one entry per query it ever read.
    #[test]
    fn forgetting_a_shuffle_drops_its_partition_count() {
        lock_partition_counts().insert(0xdead_beef, 128);
        lock_partition_counts().insert(0xfeed_face, 64);
        assert_eq!(remembered_partition_count(0xdead_beef), Some(128));

        super::super::forget_shuffle(0xdead_beef);
        assert_eq!(remembered_partition_count(0xdead_beef), None);
        assert_eq!(remembered_partition_count(0xfeed_face), Some(64));
        forget_partition_count(0xfeed_face);
    }

    #[tokio::test]
    async fn missing_map_file_reports_its_path() -> DaftResult<()> {
        let dir = tempdir("missing");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 404,
            attempt: 0xdead,
        };
        let err = read_rows(root, 3, &[input], 0, schema).await.unwrap_err();
        assert!(
            err.to_string().contains("map_404_000000000000dead.arrow"),
            "error should name the missing file, got: {}",
            err
        );
        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// Locate the committed file and return (path, partition-2 entry).
    async fn committed_file(root: &str, input: MapInput) -> (String, index::PartitionEntry) {
        let path = shared_map_file(root, 77, input.input_id, input.attempt);
        let mut f = std::fs::File::open(&path).unwrap();
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes).unwrap();
        let entry = index::partition_entry(&bytes, 2, &path).unwrap();
        (path, entry)
    }

    /// A writer that died before its data reached the mount leaves a file whose
    /// tail is missing. That must read as an error, not as fewer rows.
    #[tokio::test]
    async fn truncated_file_is_an_error_not_fewer_rows() -> DaftResult<()> {
        let dir = tempdir("truncated");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 1,
            attempt: 1,
        };
        write_shared(root, 77, input, schema.clone(), None, 50).await?;
        let (path, entry) = committed_file(root, input).await;

        // Cut the file in the middle of partition 2's range.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(entry.start + entry.len() / 2).unwrap();

        let err = read_rows(root, 77, &[input], 2, schema.clone())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("incomplete") || err.contains("early eof") || err.contains("checksum"),
            "truncation must surface as an error, got: {}",
            err
        );
        // Partitions before the cut are unaffected.
        assert_eq!(
            read_rows(root, 77, &[input], 0, schema).await?,
            expected(50)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// A hole where data should be — pages that never reached the mount — decodes
    /// as zeros, which look like a valid (if odd) record batch body. Only the
    /// checksum catches it.
    #[tokio::test]
    async fn zeroed_hole_inside_a_message_fails_the_checksum() -> DaftResult<()> {
        let dir = tempdir("hole");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 2,
            attempt: 2,
        };
        write_shared(root, 77, input, schema.clone(), None, 50).await?;
        let (path, entry) = committed_file(root, input).await;

        // Zero the last 8 *non-zero* bytes of the range. The body ends in
        // alignment padding that is already zero, so a fixed offset from the end
        // could land on bytes a hole would not change; the last non-zero byte is
        // real data inside the final message body, past all IPC framing, so only
        // values change and the parser stays happy.
        let mut bytes = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        let range = &bytes[entry.start as usize..entry.end as usize];
        let last_nonzero = range.iter().rposition(|b| *b != 0).unwrap();
        assert!(
            last_nonzero >= 8,
            "range too small for the test to be meaningful"
        );
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(entry.start + (last_nonzero as u64) - 7))
            .unwrap();
        f.write_all(&[0u8; 8]).unwrap();
        drop(f);

        let err = read_rows(root, 77, &[input], 2, schema)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("checksum"),
            "expected a checksum failure, got: {}",
            err
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// A hole that lands on a message boundary reads as end-of-stream, which the
    /// byte count catches even before the checksum is consulted.
    #[tokio::test]
    async fn zeroed_message_header_is_detected_as_incomplete() -> DaftResult<()> {
        let dir = tempdir("hole_boundary");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 4,
            attempt: 4,
        };
        write_shared(root, 77, input, schema.clone(), None, 50).await?;
        let (path, entry) = committed_file(root, input).await;

        // Zero the first message's framing so the parser sees a zero length.
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(entry.start)).unwrap();
        f.write_all(&[0u8; 8]).unwrap();
        drop(f);

        let err = read_rows(root, 77, &[input], 2, schema)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("incomplete"),
            "expected an incompleteness error, got: {}",
            err
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }
}
