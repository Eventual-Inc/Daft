use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use common_runtime::get_io_runtime;
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    errors::Result as ParquetResult,
    file::{
        metadata::ParquetMetaData,
        reader::{ChunkReader, Length},
    },
};
use snafu::ResultExt;

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

    let path: Arc<str> = Arc::from(uri);
    let meta = ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new())
        .with_context(|_| ParquetMetadataSnafu {
            path: uri.to_string(),
        })?;
    let builder = ChunkSourceBuilder::Remote(RemoteChunkSourcePrep {
        path,
        uri: uri.to_string(),
        file_size,
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

/// Deferred source-opening state. It contains no data ranges; those are made
/// only after the final read plan resolves top-level fields to parquet leaves.
pub(crate) enum ChunkSourceBuilder {
    Local(LocalChunkSource),
    Remote(RemoteChunkSourcePrep),
}

pub(crate) struct RemoteChunkSourcePrep {
    pub(super) path: Arc<str>,
    pub(super) uri: String,
    pub(super) file_size: usize,
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

    /// Produce a source-neutral immutable row-group plan. This is deliberately
    /// called only after `ColumnPlan` has been resolved: `active_leaves` are
    /// physical parquet leaves, not user-facing arrow field indices.
    pub(super) fn build_plan(
        self,
        parquet_metadata: &Arc<ParquetMetaData>,
        rg_indices: &[usize],
        predicate_leaves: &[usize],
        data_leaves: &[usize],
    ) -> RowGroupSourcePlan {
        match self {
            Self::Local(source) => RowGroupSourcePlan::Local(LocalChunkSourcePlan::from_metadata(
                source,
                parquet_metadata,
                rg_indices,
                predicate_leaves,
                data_leaves,
            )),
            Self::Remote(prep) => RowGroupSourcePlan::Remote(RemoteChunkSourcePlan::from_metadata(
                prep,
                parquet_metadata,
                rg_indices,
                predicate_leaves,
                data_leaves,
            )),
        }
    }
}

/// Decoder-facing handle returned by [`ChunkSource::open_rg`]. Per-column
/// decoders call [`Self::read_col`] without caring whether the bytes are
/// pre-fetched or fetched on demand.
#[derive(Clone)]
pub(crate) enum RgReader {
    /// Owned mode: cloning this clones the `Arc<ResidentRowGroup>` — every
    /// column decoder task keeps the resident owner (bytes + budget permit)
    /// alive for its entire lifetime. Type-level lifetime binding: the
    /// permit cannot be returned while any decoder still reads the bytes.
    Resident(Arc<ResidentRowGroup>),
}

impl RgReader {
    pub(super) async fn read_col(
        &self,
        _col_leaves: Arc<[usize]>,
    ) -> crate::Result<Arc<HashMap<usize, OffsetBytes>>> {
        match self {
            Self::Resident(resident) => Ok(resident.leaves.clone()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LocalChunkSource {
    pub(super) path: Arc<str>,
    pub(super) file: Arc<std::fs::File>,
    pub(super) file_len: u64,
}

#[cfg(not(any(unix, windows)))]
compile_error!(
    "LocalChunkSource needs FileExt::read_at (unix) or seek_read (windows); \
     no implementation for this target."
);

impl LocalChunkSource {
    const MAX_COALESCE_GAP: u64 = 64 * 1024;

    /// Read an immutable occurrence range plan in one blocking operation.
    /// The returned master group buffers are the authoritative compressed-byte
    /// owners; leaf slices in the map share those allocations.
    fn read_range_groups_sync(
        &self,
        rg_idx: usize,
        groups: Vec<RangeGroup>,
    ) -> crate::Result<(Vec<Bytes>, Arc<HashMap<usize, OffsetBytes>>)> {
        let leaf_count: usize = groups.iter().map(|g| g.members.len()).sum();
        let mut out = HashMap::with_capacity(leaf_count);
        let mut group_bytes_out = Vec::with_capacity(groups.len());
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
                let mut read = 0;
                while read < buf.len() {
                    let n = self
                        .file
                        .read_at(&mut buf[read..], group_start + read as u64)
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
                    if n == 0 {
                        return Err(crate::Error::LocalIO {
                            path: self.path.to_string(),
                            source: std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "pread for rg={} stopped at byte {} of coalesced range {}..{}",
                                    rg_idx, read, group_start, group_end
                                ),
                            ),
                        });
                    }
                    read += n;
                }
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;
                let mut read = 0;
                while read < buf.len() {
                    let n = self
                        .file
                        .seek_read(&mut buf[read..], group_start + read as u64)
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
                    if n == 0 {
                        return Err(crate::Error::LocalIO {
                            path: self.path.to_string(),
                            source: std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "seek_read for rg={} stopped at byte {} of coalesced range {}..{}",
                                    rg_idx, read, group_start, group_end
                                ),
                            ),
                        });
                    }
                    read += n;
                }
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
            group_bytes_out.push(group_bytes);
        }
        Ok((group_bytes_out, Arc::new(out)))
    }
}

/// Remote coalescing policy is shared by both predicate and data phase plans.
struct RemoteRangeLayout;

impl RemoteRangeLayout {
    const MAX_COALESCE_GAP: u64 = 1024 * 1024;
    const SPLIT_THRESHOLD: u64 = 24 * 1024 * 1024;
    const MAX_REQUEST_SIZE: u64 = 16 * 1024 * 1024;

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

/// Immutable per-RG download plan: coalesced byte ranges and the leaf → range
/// mapping. Holds no `Bytes` and spawns nothing.
pub(crate) struct RgRangePlan {
    groups: Vec<RangeGroup>,
    /// Checked sum of group byte lengths — the budget weight for this RG.
    total_bytes: usize,
}

/// The two decoding phases deliberately have separate layouts.  This keeps a
/// data-only column completely cold when the pushed predicate rejects an RG.
/// A coalesced range straddling predicate and data leaves can be fetched twice;
/// that is preferable to making predicate rejection download data bytes and is
/// confined to that one occurrence.
pub(crate) struct OccurrenceRangePlan {
    predicate: RgRangePlan,
    data: RgRangePlan,
}

#[derive(Copy, Clone)]
pub(crate) enum RangePhase {
    Predicate,
    Data,
}

impl OccurrenceRangePlan {
    fn phase(&self, phase: RangePhase) -> &RgRangePlan {
        match phase {
            RangePhase::Predicate => &self.predicate,
            RangePhase::Data => &self.data,
        }
    }
}

fn range_plan(
    metadata: &ParquetMetaData,
    rg_idx: usize,
    leaves: &[usize],
    max_gap: u64,
    split_remote: bool,
) -> RgRangePlan {
    let rg = metadata.row_group(rg_idx);
    let leaf_ranges = leaves
        .iter()
        .map(|&leaf| {
            let (start, len) = rg.column(leaf).byte_range();
            LeafRange { leaf, start, len }
        })
        .collect();
    let groups = if split_remote {
        RemoteRangeLayout::coalesce_and_split(leaf_ranges)
    } else {
        coalesce_ranges(leaf_ranges, max_gap)
    };
    let total_bytes = groups
        .iter()
        .map(|g| usize::try_from(g.end - g.start).expect("range length exceeds usize"))
        .try_fold(0usize, |acc, len| acc.checked_add(len))
        .expect("row-group byte ranges overflow usize");
    RgRangePlan {
        groups,
        total_bytes,
    }
}

/// Source-neutral occurrence plan used by the single bounded coordinator.
/// Only this enum branches on local-vs-remote; scheduling, predicate and
/// decoder lifetime live above it.
pub(crate) enum RowGroupSourcePlan {
    Local(LocalChunkSourcePlan),
    Remote(RemoteChunkSourcePlan),
}

impl RowGroupSourcePlan {
    pub(super) fn phase_bytes(&self, occurrence: usize, phase: RangePhase) -> usize {
        match self {
            Self::Local(p) => p.phase_bytes(occurrence, phase),
            Self::Remote(p) => p.phase_bytes(occurrence, phase),
        }
    }

    pub(super) async fn download(
        &self,
        occurrence: usize,
        rg_idx: usize,
        phase: RangePhase,
        permit: BudgetPermit,
    ) -> crate::Result<ResidentRowGroup> {
        match self {
            Self::Local(p) => p.download(occurrence, rg_idx, phase, permit).await,
            Self::Remote(p) => p.download(occurrence, phase, permit).await,
        }
    }
}

pub(crate) struct LocalChunkSourcePlan {
    source: LocalChunkSource,
    per_occurrence: Vec<OccurrenceRangePlan>,
}

impl LocalChunkSourcePlan {
    fn from_metadata(
        source: LocalChunkSource,
        metadata: &ParquetMetaData,
        rg_indices: &[usize],
        predicate_leaves: &[usize],
        data_leaves: &[usize],
    ) -> Self {
        let per_occurrence = rg_indices
            .iter()
            .map(|&rg_idx| OccurrenceRangePlan {
                predicate: range_plan(
                    metadata,
                    rg_idx,
                    predicate_leaves,
                    LocalChunkSource::MAX_COALESCE_GAP,
                    false,
                ),
                data: range_plan(
                    metadata,
                    rg_idx,
                    data_leaves,
                    LocalChunkSource::MAX_COALESCE_GAP,
                    false,
                ),
            })
            .collect();
        Self {
            source,
            per_occurrence,
        }
    }

    fn phase_bytes(&self, occurrence: usize, phase: RangePhase) -> usize {
        self.per_occurrence[occurrence].phase(phase).total_bytes
    }

    async fn download(
        &self,
        occurrence: usize,
        rg_idx: usize,
        phase: RangePhase,
        permit: BudgetPermit,
    ) -> crate::Result<ResidentRowGroup> {
        let source = self.source.clone();
        let plan = &self.per_occurrence[occurrence].phase(phase).groups;
        let groups = plan
            .iter()
            .map(|g| RangeGroup {
                start: g.start,
                end: g.end,
                members: g.members.clone(),
            })
            .collect::<Vec<_>>();
        let path = source.path.clone();
        let file_len = source.file_len;
        let (group_bytes, leaves) = get_io_runtime(true)
            .spawn_blocking(move || source.read_range_groups_sync(rg_idx, groups))
            .await
            .map_err(task_err(path.to_string()))??;
        Ok(ResidentRowGroup::new(group_bytes, leaves, file_len, permit))
    }
}

/// Remote fetch context plus one [`RgRangePlan`] per active row group
/// *occurrence position*. Keyed by
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
    per_occurrence: Vec<OccurrenceRangePlan>,
}

impl RemoteChunkSourcePlan {
    pub(super) fn from_metadata(
        prep: RemoteChunkSourcePrep,
        parquet_metadata: &Arc<ParquetMetaData>,
        rg_indices: &[usize],
        predicate_leaves: &[usize],
        data_leaves: &[usize],
    ) -> Self {
        let per_occurrence = rg_indices
            .iter()
            .map(|&rg_idx| OccurrenceRangePlan {
                predicate: range_plan(parquet_metadata, rg_idx, predicate_leaves, 0, true),
                data: range_plan(parquet_metadata, rg_idx, data_leaves, 0, true),
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

    /// Budget weight of one phase of an RG occurrence.
    pub(super) fn phase_bytes(&self, occurrence: usize, phase: RangePhase) -> usize {
        self.per_occurrence[occurrence].phase(phase).total_bytes
    }

    /// Download every coalesced range of one RG occurrence and assemble the
    /// RAII owner. Concurrency: all GETs of this RG run together via
    /// `try_join_all` — actual connection concurrency is bounded by the IO
    /// client's own pool semaphore (`max_connections_per_io_thread`, see
    /// `s3_like.rs`), so no extra limiter here. Any failed GET cancels the
    /// remaining futures; the permit is released by the caller dropping it.
    pub(super) async fn download(
        &self,
        occurrence: usize,
        phase: RangePhase,
        permit: BudgetPermit,
    ) -> crate::Result<ResidentRowGroup> {
        let plan = self.per_occurrence[occurrence].phase(phase);
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

        debug_assert_eq!(actual_bytes, plan.total_bytes);
        Ok(ResidentRowGroup::new(
            group_bytes,
            Arc::new(leaves),
            self.file_len,
            permit,
        ))
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

impl ResidentRowGroup {
    fn new(
        group_bytes: Vec<Bytes>,
        leaves: Arc<HashMap<usize, OffsetBytes>>,
        _file_len: u64,
        permit: BudgetPermit,
    ) -> Self {
        let actual_bytes = group_bytes.iter().map(Bytes::len).sum();
        permit.metrics().record_resident(actual_bytes);
        if super::budget::mem_verbose() {
            eprintln!(
                "[parquet-mem] +resident bytes={} {}",
                actual_bytes,
                permit.metrics().snapshot_line()
            );
        }
        Self {
            group_bytes,
            leaves,
            actual_bytes,
            permit,
        }
    }
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
    Resident(Arc<ResidentRowGroup>),
}

impl RgAccess {
    pub(super) async fn open_rg(
        &self,
        _rg_idx: usize,
        _all_leaves: Arc<[usize]>,
    ) -> crate::Result<RgReader> {
        match self {
            Self::Resident(r) => Ok(RgReader::Resident(r.clone())),
        }
    }
}
