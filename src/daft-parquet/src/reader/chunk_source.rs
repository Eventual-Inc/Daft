use std::{collections::HashMap, pin::Pin, sync::Arc};

use bytes::Bytes;
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_dsl::optimization::get_required_columns;
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    errors::Result as ParquetResult,
    file::{
        metadata::ParquetMetaData,
        reader::{ChunkReader, Length},
    },
};
use snafu::{OptionExt, ResultExt};

use crate::{
    ParquetMetadataSnafu, ReaderInternalSnafu,
    metadata::apply_field_ids_to_arrowrs_parquet_metadata, read::ParquetReadOptions, task_err,
};

fn coalesce_ranges(mut leaf_ranges: Vec<LeafRange>, max_gap: u64) -> Vec<RangeGroup> {
    leaf_ranges.sort_by_key(|r| r.start);
    let mut groups: Vec<RangeGroup> = Vec::new();
    for entry in leaf_ranges {
        let entry_end = entry.start + entry.len;
        if let Some(group) = groups.last_mut()
            && entry.start <= group.end + max_gap
        {
            group.end = group.end.max(entry_end);
            group.members.push(entry);
            continue;
        }
        groups.push(RangeGroup {
            start: entry.start,
            end: entry_end,
            members: vec![entry],
        });
    }
    groups
}

async fn drive_group(slot: &GroupSlot, path: &str) -> GroupResult {
    let mut guard = slot.state.lock().await;
    if let RangeState::Ready(r) = &*guard {
        return r.clone();
    }
    // Drive InFlight → Ready. Holding the lock across `.await` serializes
    // concurrent waiters, but they'd have had to wait for the same spawned
    // task to finish either way — no extra latency.
    //
    // `Pin::new(task).await` polls the task to completion without consuming
    // it (RuntimeTask is Unpin), so the borrow ends with the inner block and
    // we can then write the Ready result back through the same guard.
    let res: GroupResult = {
        let RangeState::InFlight(task) = &mut *guard else {
            unreachable!("Ready branch returned above")
        };
        match Pin::new(task).await {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(e)) => Err(Arc::new(e)),
            Err(daft_err) => Err(Arc::new(task_err(path.to_string())(daft_err))),
        }
    };
    *guard = RangeState::Ready(res.clone());
    res
}

pub(super) async fn open_local_file(
    path: &str,
) -> crate::Result<(Arc<std::fs::File>, u64, ArrowReaderMetadata)> {
    let path_owned = path.to_string();
    let path_for_join = path.to_string();
    get_io_runtime(true)
        .spawn_blocking(move || {
            let file = std::fs::File::open(&path_owned).map_err(|e| crate::Error::LocalIO {
                path: path_owned.clone(),
                source: e,
            })?;
            let file_len = file
                .metadata()
                .map_err(|e| crate::Error::LocalIO {
                    path: path_owned.clone(),
                    source: e,
                })?
                .len();
            let meta =
                ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).with_context(|_| {
                    ParquetMetadataSnafu {
                        path: path_owned.clone(),
                    }
                })?;
            crate::Result::Ok((Arc::new(file), file_len, meta))
        })
        .await
        .map_err(task_err(path_for_join))?
}

pub(super) async fn prepare_remote_chunk_source(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
    opts: &ParquetReadOptions,
) -> crate::Result<(ChunkSourceBuilder, ArrowReaderMetadata)> {
    let (parquet_metadata_res, file_size_res) = Box::pin(futures::future::join(
        crate::metadata::read_parquet_metadata(
            uri,
            None,
            io_client.clone(),
            io_stats.clone(),
            None,
            None,
        ),
        io_client.single_url_get_size(uri.to_string(), io_stats.clone()),
    ))
    .await;
    let mut parquet_metadata = parquet_metadata_res?;
    let file_size = file_size_res?;

    // Apply Iceberg field-id mapping before filtering by column name —
    // otherwise the prefetch matches pre-rename names against post-rename
    // user-supplied names and fetches zero leaves.
    if let Some(mapping) = opts.field_id_mapping.as_deref() {
        parquet_metadata =
            apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping, uri)?;
    }

    // Prefetch needs the union of user-requested columns and predicate columns.
    let prefetch_col_names: Option<std::collections::HashSet<String>> =
        opts.columns.as_ref().map(|cols| {
            let mut acc: std::collections::HashSet<String> = cols.iter().cloned().collect();
            if let Some(pred) = opts.predicate.as_ref() {
                acc.extend(get_required_columns(pred));
            }
            acc
        });

    let schema_descr = parquet_metadata.file_metadata().schema_descr();
    let num_cols_total = schema_descr.num_columns();
    let active_col_indices: Vec<usize> = match &prefetch_col_names {
        None => (0..num_cols_total).collect(),
        Some(want) => (0..num_cols_total)
            .filter(|&i| {
                schema_descr
                    .column(i)
                    .path()
                    .parts()
                    .first()
                    .map(|n| want.contains(n.as_str()))
                    .unwrap_or(false)
            })
            .collect(),
    };

    let path: Arc<str> = Arc::from(uri);
    let meta = ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new())
        .with_context(|_| ParquetMetadataSnafu {
            path: uri.to_string(),
        })?;
    let builder = ChunkSourceBuilder::Remote(RemoteChunkSourcePrep {
        path,
        uri: uri.to_string(),
        file_size,
        active_col_indices,
        io_client,
        io_stats,
    });
    Ok((builder, meta))
}

#[derive(Copy, Clone)]
struct LeafRange {
    leaf: usize,
    start: u64,
    len: u64,
}

struct RangeGroup {
    start: u64,
    end: u64,
    members: Vec<LeafRange>,
}

/// `ChunkReader` windowed over a single column chunk's bytes. Reports the
/// file's total length but translates absolute offsets to local-buffer offsets,
/// so `SerializedPageReader` works without holding the whole file in memory.
#[derive(Clone)]
pub(crate) struct OffsetBytes {
    base: u64,
    file_len: u64,
    bytes: Bytes,
}

impl Length for OffsetBytes {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for OffsetBytes {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let local = start.checked_sub(self.base).ok_or_else(|| {
            parquet::errors::ParquetError::General(format!(
                "OffsetBytes::get_read: start {} < base {}",
                start, self.base
            ))
        })? as usize;
        if local > self.bytes.len() {
            return Err(parquet::errors::ParquetError::General(format!(
                "OffsetBytes::get_read: start {} past chunk end (local {} > len {})",
                start,
                local,
                self.bytes.len()
            )));
        }
        use bytes::Buf;
        Ok(self.bytes.slice(local..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let local = start.checked_sub(self.base).ok_or_else(|| {
            parquet::errors::ParquetError::General(format!(
                "OffsetBytes::get_bytes: start {} < base {}",
                start, self.base
            ))
        })? as usize;
        let end = local.checked_add(length).ok_or_else(|| {
            parquet::errors::ParquetError::General("OffsetBytes::get_bytes: offset overflow".into())
        })?;
        if end > self.bytes.len() {
            return Err(parquet::errors::ParquetError::General(format!(
                "OffsetBytes::get_bytes: range {}..{} past chunk end (len {})",
                local,
                end,
                self.bytes.len()
            )));
        }
        Ok(self.bytes.slice(local..end))
    }
}

/// Per-(rg, leaf) factory for column-chunk byte slices. Enum-dispatched (rather
/// than `dyn`) so `read_rg_chunks` can be a plain async fn — no per-call
/// `BoxFuture` allocation.
pub(crate) enum ChunkSource {
    Local(LocalChunkSource),
    Remote(RemoteChunkSource),
}

/// Deferred construction of a `ChunkSource`. For `Local` the source is already
/// open (`open_local_file` is a cheap syscall + footer read). For `Remote` we
/// hold the bits needed to spawn byte-range fetches but have NOT spawned them
/// yet — caller passes a final, predicate-pruned RG set to [`Self::build`].
pub(crate) enum ChunkSourceBuilder {
    Local(LocalChunkSource),
    Remote(RemoteChunkSourcePrep),
}

pub(crate) struct RemoteChunkSourcePrep {
    pub(super) path: Arc<str>,
    pub(super) uri: String,
    pub(super) file_size: usize,
    pub(super) active_col_indices: Vec<usize>,
    pub(super) io_client: Arc<daft_io::IOClient>,
    pub(super) io_stats: Option<daft_io::IOStatsRef>,
}

impl ChunkSourceBuilder {
    pub(super) fn path(&self) -> &Arc<str> {
        match self {
            Self::Local(s) => &s.path,
            Self::Remote(p) => &p.path,
        }
    }

    /// Owned-mode counterpart of [`Self::build`]: produce an immutable range
    /// *plan* (no GETs spawned, no `Bytes` held) for the remote source.
    /// Returns `Err(self)` for local sources — the caller falls back to the
    /// legacy path (local reads are per-call `pread`s with no resident
    /// cache, so the ownership model buys nothing there this round).
    pub(super) fn build_plan(
        self,
        parquet_metadata: &Arc<ParquetMetaData>,
        rg_indices: &[usize],
    ) -> Result<RemoteChunkSourcePlan, Box<Self>> {
        match self {
            Self::Local(_) => Err(Box::new(self)),
            Self::Remote(prep) => Ok(RemoteChunkSourcePlan::from_metadata(
                prep,
                parquet_metadata,
                rg_indices,
            )),
        }
    }

    /// Finalize the chunk source. For `Remote`, this is when byte-range fetches
    /// for `rg_indices` are spawned — call only after predicate-based pruning.
    pub(super) fn build(
        self,
        parquet_metadata: Arc<ParquetMetaData>,
        rg_indices: &[usize],
    ) -> ChunkSource {
        match self {
            Self::Local(cs) => ChunkSource::Local(cs),
            Self::Remote(prep) => ChunkSource::Remote(RemoteChunkSource::from_ranged(
                prep.path,
                parquet_metadata,
                prep.file_size,
                &prep.active_col_indices,
                rg_indices,
                prep.io_client,
                prep.io_stats,
                prep.uri,
            )),
        }
    }
}

impl ChunkSource {
    pub(super) async fn read_rg_chunks(
        &self,
        rg_idx: usize,
        leaves: Arc<[usize]>,
    ) -> crate::Result<HashMap<usize, OffsetBytes>> {
        match self {
            // Local pread is sync; offload to the IO runtime's blocking pool so
            // we don't park a tokio compute worker on syscalls under heavy
            // concurrency.
            Self::Local(s) => {
                let s = s.clone();
                let path = s.path.clone();
                get_io_runtime(true)
                    .spawn_blocking(move || s.read_rg_chunks_sync(rg_idx, &leaves))
                    .await
                    .map_err(task_err(path.to_string()))?
            }
            Self::Remote(s) => s.read_rg_chunks(rg_idx, &leaves).await,
        }
    }

    /// One per RG, called by the decoder dispatch. Lets each `ChunkSource`
    /// variant choose its own access pattern without leaking the dispatch into
    /// callers:
    ///
    /// - `Local`: one batched `spawn_blocking` for *all* requested leaves, then
    ///   every column decoder reads slices from the same `Arc<HashMap>`. The
    ///   alternative (per-column reads) would multiply `spawn_blocking` calls
    ///   by `num_cols`, dominating wide-schema runtimes.
    /// - `Remote`: returns a lazy handle. Each decoder later asks for its own
    ///   leaves and awaits only the coalesced byte-range groups covering them,
    ///   so fast columns start streaming while slower groups are still in
    ///   flight.
    pub(super) async fn open_rg(
        self: Arc<Self>,
        rg_idx: usize,
        all_leaves: Arc<[usize]>,
    ) -> crate::Result<RgReader> {
        match self.as_ref() {
            Self::Local(_) => {
                let chunks = self.read_rg_chunks(rg_idx, all_leaves).await?;
                Ok(RgReader::PreFetched(Arc::new(chunks)))
            }
            Self::Remote(_) => Ok(RgReader::Lazy {
                chunk_source: self,
                rg_idx,
            }),
        }
    }
}

/// Decoder-facing handle returned by [`ChunkSource::open_rg`]. Per-column
/// decoders call [`Self::read_col`] without caring whether the bytes are
/// pre-fetched or fetched on demand.
#[derive(Clone)]
pub(crate) enum RgReader {
    PreFetched(Arc<HashMap<usize, OffsetBytes>>),
    Lazy {
        chunk_source: Arc<ChunkSource>,
        rg_idx: usize,
    },
    /// Owned mode: cloning this clones the `Arc<ResidentRowGroup>` — every
    /// column decoder task keeps the resident owner (bytes + budget permit)
    /// alive for its entire lifetime. Type-level lifetime binding: the
    /// permit cannot be returned while any decoder still reads the bytes.
    Resident(Arc<ResidentRowGroup>),
}

impl RgReader {
    pub(super) async fn read_col(
        &self,
        col_leaves: Arc<[usize]>,
    ) -> crate::Result<Arc<HashMap<usize, OffsetBytes>>> {
        match self {
            Self::PreFetched(map) => Ok(map.clone()),
            Self::Lazy {
                chunk_source,
                rg_idx,
            } => Ok(Arc::new(
                chunk_source.read_rg_chunks(*rg_idx, col_leaves).await?,
            )),
            Self::Resident(resident) => Ok(resident.leaves.clone()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LocalChunkSource {
    pub(super) path: Arc<str>,
    pub(super) file: Arc<std::fs::File>,
    pub(super) file_len: u64,
    pub(super) metadata: Arc<ParquetMetaData>,
}

#[cfg(not(any(unix, windows)))]
compile_error!(
    "LocalChunkSource needs FileExt::read_at (unix) or seek_read (windows); \
     no implementation for this target."
);

impl LocalChunkSource {
    const MAX_COALESCE_GAP: u64 = 64 * 1024;

    fn read_rg_chunks_sync(
        &self,
        rg_idx: usize,
        leaves: &[usize],
    ) -> crate::Result<HashMap<usize, OffsetBytes>> {
        if leaves.is_empty() {
            return Ok(HashMap::new());
        }
        let rg = self.metadata.row_group(rg_idx);
        let leaf_ranges: Vec<LeafRange> = leaves
            .iter()
            .map(|&l| {
                let (start, len) = rg.column(l).byte_range();
                LeafRange {
                    leaf: l,
                    start,
                    len,
                }
            })
            .collect();
        let groups = coalesce_ranges(leaf_ranges, Self::MAX_COALESCE_GAP);
        let mut out = HashMap::with_capacity(leaves.len());
        for RangeGroup {
            start: group_start,
            end: group_end,
            members,
        } in groups
        {
            let group_len = (group_end - group_start) as usize;
            let mut buf = vec![0u8; group_len];
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                self.file
                    .read_at(&mut buf, group_start)
                    .map_err(|e| crate::Error::LocalIO {
                        path: self.path.to_string(),
                        source: std::io::Error::new(
                            e.kind(),
                            format!(
                                "pread for rg={} coalesced range {}..{}: {}",
                                rg_idx, group_start, group_end, e
                            ),
                        ),
                    })?;
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;
                self.file
                    .seek_read(&mut buf, group_start)
                    .map_err(|e| crate::Error::LocalIO {
                        path: self.path.to_string(),
                        source: std::io::Error::new(
                            e.kind(),
                            format!(
                                "seek_read for rg={} coalesced range {}..{}: {}",
                                rg_idx, group_start, group_end, e
                            ),
                        ),
                    })?;
            }
            let group_bytes = Bytes::from(buf);
            for LeafRange { leaf, start, len } in members {
                let local_start = (start - group_start) as usize;
                let local_end = local_start + len as usize;
                let slice = group_bytes.slice(local_start..local_end);
                out.insert(
                    leaf,
                    OffsetBytes {
                        base: start,
                        file_len: self.file_len,
                        bytes: slice,
                    },
                );
            }
        }
        Ok(out)
    }
}

type SharedErr = Arc<crate::Error>;
type GroupResult = Result<Bytes, SharedErr>;

/// Per-coalesced-byte-range fetch state. Starts as `InFlight(task)`; the
/// first awaiter drives the spawned task to completion and stores the
/// (cloneable) result in `Ready`. Subsequent awaiters clone the cached
/// bytes/error instead of re-fetching.
enum RangeState {
    InFlight(RuntimeTask<crate::Result<Bytes>>),
    Ready(GroupResult),
}

/// One coalesced byte-range within a row group: the absolute file offset its
/// bytes start at, plus the lazily-resolved fetch state.
struct GroupSlot {
    group_start: u64,
    state: tokio::sync::Mutex<RangeState>,
}

/// Where a leaf's bytes live: which coalesced group, and its absolute slice
/// within that group's bytes. We slice on demand at read time rather than
/// pre-building a per-leaf `(start, Bytes)` map.
struct LeafLoc {
    group_idx: usize,
    leaf_start: u64,
    leaf_len: u64,
}

struct RgState {
    groups: Vec<GroupSlot>,
    leaves: HashMap<usize, LeafLoc>,
}

/// Per-row-group coalesced byte-range GETs (merge ≤1MB gaps, split >24MB at
/// chunk boundaries into ~16MB pieces). Each coalesced group has its own
/// spawned fetch task with `InFlight → Ready` state transitions, so a
/// `read_rg_chunks` call only awaits the groups containing its requested
/// leaves — never the whole RG's bundle. Once driven, the cached `Ready`
/// bytes let a later call for a different leaf in the same group skip
/// re-fetching. Errors are wrapped in `Arc` so they can be cloned to every
/// subsequent awaiter.
pub(crate) struct RemoteChunkSource {
    path: Arc<str>,
    rgs: HashMap<usize, RgState>,
    file_len: u64,
}

impl RemoteChunkSource {
    const MAX_COALESCE_GAP: u64 = 1024 * 1024;
    const SPLIT_THRESHOLD: u64 = 24 * 1024 * 1024;
    const MAX_REQUEST_SIZE: u64 = 16 * 1024 * 1024;

    #[allow(clippy::too_many_arguments)]
    fn from_ranged(
        path: Arc<str>,
        parquet_metadata: Arc<ParquetMetaData>,
        file_size: usize,
        active_col_indices: &[usize],
        active_rg_indices: &[usize],
        io_client: Arc<daft_io::IOClient>,
        io_stats: Option<daft_io::IOStatsRef>,
        uri: String,
    ) -> Self {
        let file_len = file_size as u64;
        let io_runtime = get_io_runtime(true);
        let mut rgs = HashMap::with_capacity(active_rg_indices.len());

        for &rg_idx in active_rg_indices {
            let groups = rg_coalesced_layout(&parquet_metadata, rg_idx, active_col_indices);

            let mut group_slots: Vec<GroupSlot> = Vec::with_capacity(groups.len());
            let mut leaves: HashMap<usize, LeafLoc> =
                HashMap::with_capacity(active_col_indices.len());

            for (
                group_idx,
                RangeGroup {
                    start: group_start,
                    end: group_end,
                    members,
                },
            ) in groups.into_iter().enumerate()
            {
                let io_client = io_client.clone();
                let io_stats = io_stats.clone();
                let uri = uri.clone();
                let range = group_start as usize..group_end as usize;
                let task = io_runtime.spawn(async move {
                    let get_result = io_client
                        .single_url_get(
                            uri,
                            Some(daft_io::range::GetRange::Bounded(range)),
                            io_stats,
                        )
                        .await?;
                    let bytes = get_result.bytes().await?;
                    crate::Result::Ok(bytes)
                });
                group_slots.push(GroupSlot {
                    group_start,
                    state: tokio::sync::Mutex::new(RangeState::InFlight(task)),
                });
                for LeafRange { leaf, start, len } in members {
                    leaves.insert(
                        leaf,
                        LeafLoc {
                            group_idx,
                            leaf_start: start,
                            leaf_len: len,
                        },
                    );
                }
            }

            rgs.insert(
                rg_idx,
                RgState {
                    groups: group_slots,
                    leaves,
                },
            );
        }

        Self {
            path,
            rgs,
            file_len,
        }
    }

    fn coalesce_and_split(leaf_ranges: Vec<LeafRange>) -> Vec<RangeGroup> {
        let mut groups = coalesce_ranges(leaf_ranges, Self::MAX_COALESCE_GAP);
        let mut split_groups: Vec<RangeGroup> = Vec::with_capacity(groups.len());
        for RangeGroup {
            start: group_start,
            end: group_end,
            mut members,
        } in groups.drain(..)
        {
            if group_end - group_start <= Self::SPLIT_THRESHOLD {
                split_groups.push(RangeGroup {
                    start: group_start,
                    end: group_end,
                    members,
                });
                continue;
            }
            members.sort_by_key(|r| r.start);
            let mut piece_start = group_start;
            let mut piece_members: Vec<LeafRange> = Vec::new();
            let mut piece_end = piece_start;
            for entry in members {
                let entry_end = entry.start + entry.len;
                let would_be_size = entry_end - piece_start;
                if !piece_members.is_empty() && would_be_size > Self::MAX_REQUEST_SIZE {
                    split_groups.push(RangeGroup {
                        start: piece_start,
                        end: piece_end,
                        members: std::mem::take(&mut piece_members),
                    });
                    piece_start = entry.start;
                }
                piece_end = entry_end;
                piece_members.push(entry);
            }
            if !piece_members.is_empty() {
                split_groups.push(RangeGroup {
                    start: piece_start,
                    end: piece_end,
                    members: piece_members,
                });
            }
        }
        split_groups
    }

    async fn read_rg_chunks(
        &self,
        rg_idx: usize,
        leaves: &[usize],
    ) -> crate::Result<HashMap<usize, OffsetBytes>> {
        if leaves.is_empty() {
            return Ok(HashMap::new());
        }
        let rg = self.rgs.get(&rg_idx).with_context(|| ReaderInternalSnafu {
            path: self.path.to_string(),
            message: format!("RemoteChunkSource: no pre-spawned fetch for rg={}", rg_idx),
        })?;

        // Map each leaf to its enclosing coalesced group, then dedup so we
        // only drive each group's fetch once even when several requested
        // leaves share a group.
        let mut needed_groups: Vec<usize> = Vec::with_capacity(leaves.len());
        for &leaf in leaves {
            let loc = rg.leaves.get(&leaf).with_context(|| ReaderInternalSnafu {
                path: self.path.to_string(),
                message: format!(
                    "RemoteChunkSource: chunk not pre-fetched for rg={}, leaf={}",
                    rg_idx, leaf
                ),
            })?;
            needed_groups.push(loc.group_idx);
        }
        needed_groups.sort_unstable();
        needed_groups.dedup();

        // Drive only the groups containing requested leaves, in parallel.
        // Unrelated groups in this RG stay in-flight (or untouched) and we
        // never park on their completion.
        let path = self.path.as_ref();
        let futs = needed_groups.iter().map(|&gi| {
            let slot = &rg.groups[gi];
            async move { (gi, drive_group(slot, path).await) }
        });
        let results = futures::future::join_all(futs).await;

        let mut group_bytes: HashMap<usize, Bytes> = HashMap::with_capacity(results.len());
        for (gi, res) in results {
            let bytes = res.map_err(|source| crate::Error::RemoteFetchFailed {
                path: self.path.to_string(),
                source,
            })?;
            group_bytes.insert(gi, bytes);
        }

        let mut out = HashMap::with_capacity(leaves.len());
        for &leaf in leaves {
            let loc = rg.leaves.get(&leaf).expect("validated above");
            let bytes = group_bytes.get(&loc.group_idx).expect("driven above");
            let group_start = rg.groups[loc.group_idx].group_start;
            let local_start = (loc.leaf_start - group_start) as usize;
            let local_end = local_start + loc.leaf_len as usize;
            let slice = bytes.slice(local_start..local_end);
            out.insert(
                leaf,
                OffsetBytes {
                    base: loc.leaf_start,
                    file_len: self.file_len,
                    bytes: slice,
                },
            );
        }
        Ok(out)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Owned-mode types: immutable range plan + RAII resident row group.
//
// The legacy path above instantiates the whole-file range plan as whole-file
// resident work (every GET spawned at build, bytes cached until the reader
// drops). The owned path keeps the same coalesced layout (preserving the
// cross-column IO merging that motivated the custom reader, see PR #6952)
// but binds download and residency to a per-row-group owner admitted through
// the process-wide byte budget.
// ─────────────────────────────────────────────────────────────────────────────

use super::budget::BudgetPermit;

/// Shared between the legacy eager source and the owned plan so the IO
/// layout (coalesce ≤1MiB gaps, split >24MiB at chunk boundaries) can never
/// diverge between the two modes.
fn rg_coalesced_layout(
    metadata: &ParquetMetaData,
    rg_idx: usize,
    active_col_indices: &[usize],
) -> Vec<RangeGroup> {
    let rg = metadata.row_group(rg_idx);
    let mut leaf_ranges: Vec<LeafRange> = Vec::with_capacity(active_col_indices.len());
    for &col_idx in active_col_indices {
        let (start, len) = rg.column(col_idx).byte_range();
        leaf_ranges.push(LeafRange {
            leaf: col_idx,
            start,
            len,
        });
    }
    RemoteChunkSource::coalesce_and_split(leaf_ranges)
}

/// Immutable per-RG download plan: coalesced byte ranges and the leaf → range
/// mapping. Holds no `Bytes` and spawns nothing.
pub(crate) struct RgRangePlan {
    groups: Vec<RangeGroup>,
    /// Checked sum of group byte lengths — the budget weight for this RG.
    total_bytes: usize,
}

/// Owned-mode replacement for `RemoteChunkSource`: the fetch context plus one
/// [`RgRangePlan`] per active row group *occurrence position*. Keyed by
/// position in the pruned `rg_indices` list (NOT by `rg_idx`) so duplicate
/// row groups (`row_groups=[0, 0]`, supported since PR #6952) get fully
/// independent plans/permits/residents.
pub(crate) struct RemoteChunkSourcePlan {
    pub(super) path: Arc<str>,
    file_len: u64,
    uri: String,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
    /// Indexed by occurrence position, parallel to the pruned rg_indices.
    per_occurrence: Vec<RgRangePlan>,
}

impl RemoteChunkSourcePlan {
    pub(super) fn from_metadata(
        prep: RemoteChunkSourcePrep,
        parquet_metadata: &Arc<ParquetMetaData>,
        rg_indices: &[usize],
    ) -> Self {
        let per_occurrence = rg_indices
            .iter()
            .map(|&rg_idx| {
                let groups =
                    rg_coalesced_layout(parquet_metadata, rg_idx, &prep.active_col_indices);
                let total_bytes = groups
                    .iter()
                    .map(|g| usize::try_from(g.end - g.start).expect("range length exceeds usize"))
                    .try_fold(0usize, |acc, len| acc.checked_add(len))
                    .expect("row-group byte ranges overflow usize");
                RgRangePlan {
                    groups,
                    total_bytes,
                }
            })
            .collect();
        Self {
            path: prep.path,
            file_len: prep.file_size as u64,
            uri: prep.uri,
            io_client: prep.io_client,
            io_stats: prep.io_stats,
            per_occurrence,
        }
    }

    /// Budget weight of one RG occurrence: planned compressed bytes.
    pub(super) fn occurrence_bytes(&self, occurrence: usize) -> usize {
        self.per_occurrence[occurrence].total_bytes
    }

    /// Download every coalesced range of one RG occurrence and assemble the
    /// RAII owner. Concurrency: all GETs of this RG run together via
    /// `try_join_all` — actual connection concurrency is bounded by the IO
    /// client's own pool semaphore (`max_connections_per_io_thread`, see
    /// `s3_like.rs`), so no extra limiter here. Any failed GET cancels the
    /// remaining futures; the permit is released by the caller dropping it.
    pub(super) async fn download_occurrence(
        &self,
        occurrence: usize,
        permit: BudgetPermit,
    ) -> crate::Result<ResidentRowGroup> {
        let plan = &self.per_occurrence[occurrence];
        let fetches = plan.groups.iter().map(|g| {
            let range = g.start as usize..g.end as usize;
            let uri = self.uri.clone();
            let io_client = self.io_client.clone();
            let io_stats = self.io_stats.clone();
            async move {
                let expected = range.end - range.start;
                let get_result = io_client
                    .single_url_get(
                        uri,
                        Some(daft_io::range::GetRange::Bounded(range)),
                        io_stats,
                    )
                    .await?;
                let bytes = get_result.bytes().await?;
                Ok::<_, crate::Error>((bytes, expected))
            }
        });
        let results = futures::future::try_join_all(fetches).await?;

        let mut group_bytes = Vec::with_capacity(results.len());
        let mut actual_bytes = 0usize;
        for (i, (bytes, expected)) in results.into_iter().enumerate() {
            // A short/long body would silently corrupt slicing offsets AND
            // the budget ledger — fail with a diagnosable error instead.
            if bytes.len() != expected {
                return Err(ReaderInternalSnafu {
                    path: self.path.to_string(),
                    message: format!(
                        "range GET length mismatch for group {i}: expected {expected} bytes, got {}",
                        bytes.len()
                    ),
                }
                .build());
            }
            actual_bytes = actual_bytes
                .checked_add(bytes.len())
                .expect("resident byte total overflows usize");
            group_bytes.push(bytes);
        }

        let mut leaves = HashMap::new();
        for (group, bytes) in plan.groups.iter().zip(&group_bytes) {
            for &LeafRange { leaf, start, len } in &group.members {
                let local_start = (start - group.start) as usize;
                let slice = bytes.slice(local_start..local_start + len as usize);
                leaves.insert(
                    leaf,
                    OffsetBytes {
                        base: start,
                        file_len: self.file_len,
                        bytes: slice,
                    },
                );
            }
        }

        permit.metrics().record_resident(actual_bytes);
        log::debug!(
            "resident rg occurrence={} bytes={} path={}",
            occurrence,
            actual_bytes,
            self.path
        );
        if super::budget::mem_verbose() {
            eprintln!(
                "[parquet-mem] +resident occurrence={} bytes={} {}",
                occurrence,
                actual_bytes,
                permit.metrics().snapshot_line()
            );
        }
        Ok(ResidentRowGroup {
            group_bytes,
            leaves: Arc::new(leaves),
            actual_bytes,
            permit,
        })
    }
}

/// RAII owner of one downloaded row group occurrence. Every decoder holds
/// this via `RgReader::Resident(Arc<..>)`, so the compressed bytes AND the
/// budget permit are released exactly when the last reference disappears —
/// on success, error, panic, or abort alike. No release state machine.
pub(crate) struct ResidentRowGroup {
    /// Master references to the downloaded coalesced ranges. `leaves` holds
    /// refcounted slices of these same allocations — metrics must count
    /// `group_bytes` lengths only (leaf slices would double-count shared
    /// coalesced regions).
    #[allow(dead_code)]
    group_bytes: Vec<Bytes>,
    pub(super) leaves: Arc<HashMap<usize, OffsetBytes>>,
    actual_bytes: usize,
    permit: BudgetPermit,
}

impl Drop for ResidentRowGroup {
    fn drop(&mut self) {
        self.permit.metrics().release_resident(self.actual_bytes);
        log::debug!("released resident rg bytes={}", self.actual_bytes);
        if super::budget::mem_verbose() {
            eprintln!(
                "[parquet-mem] -resident bytes={} {}",
                self.actual_bytes,
                self.permit.metrics().snapshot_line()
            );
        }
        // `permit` field drops after this, returning the planned bytes to
        // the budget and waking the queue front.
    }
}

/// Per-RG chunk access handed to the decoders: either the legacy shared
/// source (lazy per-column reads) or an owned resident row group.
pub(crate) enum RgAccess {
    Source(Arc<ChunkSource>),
    Resident(Arc<ResidentRowGroup>),
}

impl RgAccess {
    pub(super) async fn open_rg(
        &self,
        rg_idx: usize,
        all_leaves: Arc<[usize]>,
    ) -> crate::Result<RgReader> {
        match self {
            Self::Source(cs) => cs.clone().open_rg(rg_idx, all_leaves).await,
            Self::Resident(r) => Ok(RgReader::Resident(r.clone())),
        }
    }
}
