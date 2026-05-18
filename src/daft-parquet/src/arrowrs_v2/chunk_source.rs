//! Byte sources for the v2 parquet reader.
//!
//! `OffsetBytes` is a `ChunkReader` windowed over a single column chunk's
//! bytes; `LocalChunkSource` does positioned `pread`s (coalesced) per RG;
//! `RemoteChunkSource` pre-spawns coalesced range GETs at construction time
//! and serves decoders from in-memory `Bytes` slices.
//!
//! `open_local_file` and `fetch_remote_chunk_source` are the two top-level
//! constructors used by `parquet_stream_v2`.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use common_error::DaftResult;
use daft_core::prelude::*;
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    errors::Result as ParquetResult,
    file::{
        metadata::ParquetMetaData,
        reader::{ChunkReader, Length},
    },
};

use super::util::parquet_err;
use crate::metadata::apply_field_ids_to_arrowrs_parquet_metadata;

/// One leaf's byte range within the file: `(leaf_idx, start, len)`.
type LeafRange = (usize, u64, u64);
/// A coalesced run of byte ranges: `(run_start, run_end, leaves_in_run)`.
type RangeGroup = (u64, u64, Vec<LeafRange>);

// ---------------------------------------------------------------------------
// OffsetBytes: ChunkReader windowed over a per-column-chunk byte slice.
//
// SerializedPageReader works in absolute file offsets (the values stored in
// the column chunk metadata). OffsetBytes holds only ONE column chunk's bytes
// but reports the FILE's total length via Length::len, and translates absolute
// offsets to local-buffer offsets in get_read/get_bytes. This lets us avoid
// holding the whole file in memory.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ChunkSource: per-(rg, leaf) factory for the column-chunk byte slice each
// leaf decoder reads from.
//
// LocalChunkSource: holds an Arc<File>; each call does one positioned `pread`
// for an RG's active leaves (with adjacent leaves coalesced into a single
// read). No mmap, no whole-file Bytes.
//
// RemoteChunkSource: for each RG, coalesces active leaves' byte ranges into
// runs (≤1MB gap, ≤16MB per run, splits at chunk boundaries if a run
// exceeds 24MB), spawns one task per run that does
// `single_url_get(range).bytes().await`, and assembles the resulting
// HashMap<leaf, (start, Bytes)> behind a `Shared<BoxFuture>` so decoders
// for that RG can await once and get every leaf's slice. Bytes are fully
// in RAM by the time any decoder reads from them — no per-byte pump, no
// per-leaf condvar.
// ---------------------------------------------------------------------------

/// Enum-dispatched chunk source. Using an enum (rather than `dyn ChunkSource`)
/// lets us write `async fn read_rg_chunks` directly — no per-call BoxFuture
/// alloc, which adds up to milliseconds across many RGs.
pub(super) enum ChunkSource {
    Local(LocalChunkSource),
    Remote(RemoteChunkSource),
}

impl ChunkSource {
    pub(super) async fn read_rg_chunks(
        &self,
        rg_idx: usize,
        leaves: Arc<[usize]>,
    ) -> DaftResult<std::collections::HashMap<usize, OffsetBytes>> {
        match self {
            Self::Local(s) => s.read_rg_chunks_sync(rg_idx, &leaves),
            Self::Remote(s) => s.read_rg_chunks(rg_idx, &leaves).await,
        }
    }
}

/// Coalesce sorted (leaf, start, len) tuples into runs of contiguous-ish ranges.
/// Two adjacent ranges are merged into one read if the gap between them is
/// ≤ `max_gap`. Returns (run_start, run_end, leaves-in-run) per run.
fn coalesce_ranges(mut leaf_ranges: Vec<LeafRange>, max_gap: u64) -> Vec<RangeGroup> {
    leaf_ranges.sort_by_key(|&(_, s, _)| s);
    let mut groups: Vec<RangeGroup> = Vec::new();
    for entry in leaf_ranges {
        let entry_end = entry.1 + entry.2;
        if let Some((_, end, members)) = groups.last_mut()
            && entry.1 <= *end + max_gap
        {
            *end = (*end).max(entry_end);
            members.push(entry);
            continue;
        }
        groups.push((entry.1, entry_end, vec![entry]));
    }
    groups
}

pub(super) struct LocalChunkSource {
    pub(super) file: Arc<std::fs::File>,
    pub(super) file_len: u64,
    pub(super) metadata: Arc<ParquetMetaData>,
}

impl LocalChunkSource {
    /// Maximum gap (bytes) between adjacent column chunks to coalesce into one
    /// `pread`. parquet writes col chunks back-to-back within an RG, so gaps
    /// here are usually zero. 64KB is a safe upper bound for noise.
    const MAX_COALESCE_GAP: u64 = 64 * 1024;

    fn read_rg_chunks_sync(
        &self,
        rg_idx: usize,
        leaves: &[usize],
    ) -> DaftResult<std::collections::HashMap<usize, OffsetBytes>> {
        if leaves.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let rg = self.metadata.row_group(rg_idx);
        let leaf_ranges: Vec<LeafRange> = leaves
            .iter()
            .map(|&l| {
                let (s, n) = rg.column(l).byte_range();
                (l, s, n)
            })
            .collect();
        let groups = coalesce_ranges(leaf_ranges, Self::MAX_COALESCE_GAP);
        let mut out = std::collections::HashMap::with_capacity(leaves.len());
        for (group_start, group_end, members) in groups {
            let group_len = (group_end - group_start) as usize;
            let mut buf = vec![0u8; group_len];
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                self.file.read_at(&mut buf, group_start).map_err(|e| {
                    common_error::DaftError::IoError(std::io::Error::new(
                        e.kind(),
                        format!(
                            "pread for rg={} coalesced range {}..{}: {}",
                            rg_idx, group_start, group_end, e
                        ),
                    ))
                })?;
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;
                self.file.seek_read(&mut buf, group_start).map_err(|e| {
                    common_error::DaftError::IoError(std::io::Error::new(
                        e.kind(),
                        format!(
                            "seek_read for rg={} coalesced range {}..{}: {}",
                            rg_idx, group_start, group_end, e
                        ),
                    ))
                })?;
            }
            let group_bytes = Bytes::from(buf);
            for (leaf, s, n) in members {
                let local_start = (s - group_start) as usize;
                let local_end = local_start + n as usize;
                let slice = group_bytes.slice(local_start..local_end);
                out.insert(
                    leaf,
                    OffsetBytes {
                        base: s,
                        file_len: self.file_len,
                        bytes: slice,
                    },
                );
            }
        }
        Ok(out)
    }
}

/// Remote chunk source. For each row group:
///   1. Compute each active leaf's `(start, len)` byte range.
///   2. Coalesce contiguous-ish ranges (gap ≤ 1MB, run ≤ 16MB) into GET groups.
///   3. Split oversized groups (> 24MB) at column-chunk boundaries into
///      ~16MB pieces (each leaf still belongs to exactly one piece).
///   4. Spawn one tokio task per piece that does
///      `single_url_get(range).bytes().await` — the bytes are collected in
///      full before any decoder reads from them. Decoders for that RG await
///      a `Shared<BoxFuture>` of the assembled `HashMap<leaf, (start, Bytes)>`.
///
/// Parallel range fetches at construction time, in-memory `Bytes` caching
/// per range, decoders read from cached slices. Decoders see bytes-already-
/// there and never park.
pub(super) struct RemoteChunkSource {
    rg_fetches: std::collections::HashMap<usize, SharedRgFetch>,
    file_len: u64,
}

type RgBytesMap = std::collections::HashMap<usize, (u64, Bytes)>;
type SharedRgFetch = futures::future::Shared<
    futures::future::BoxFuture<'static, Result<Arc<RgBytesMap>, Arc<common_error::DaftError>>>,
>;

impl RemoteChunkSource {
    /// Adjacent column chunks separated by ≤ MAX_COALESCE_GAP are merged
    /// into one GET. If the merged run exceeds SPLIT_THRESHOLD, it's split
    /// at column-chunk boundaries into pieces of ≤ MAX_REQUEST_SIZE.
    const MAX_COALESCE_GAP: u64 = 1024 * 1024;
    const SPLIT_THRESHOLD: u64 = 24 * 1024 * 1024;
    const MAX_REQUEST_SIZE: u64 = 16 * 1024 * 1024;

    fn from_ranged(
        parquet_metadata: Arc<ParquetMetaData>,
        file_size: usize,
        active_col_indices: &[usize],
        active_rg_indices: &[usize],
        io_client: Arc<daft_io::IOClient>,
        io_stats: Option<daft_io::IOStatsRef>,
        uri: String,
    ) -> Self {
        use futures::future::FutureExt;
        let file_len = file_size as u64;
        let mut rg_fetches = std::collections::HashMap::with_capacity(active_rg_indices.len());

        for &rg_idx in active_rg_indices {
            let rg = parquet_metadata.row_group(rg_idx);
            let mut leaf_ranges: Vec<LeafRange> = Vec::with_capacity(active_col_indices.len());
            for &col_idx in active_col_indices {
                let (start, len) = rg.column(col_idx).byte_range();
                leaf_ranges.push((col_idx, start, len));
            }
            let groups = Self::coalesce_and_split(leaf_ranges);

            // Spawn one fetch task per coalesced piece. They run concurrently.
            type FetchHandle = tokio::task::JoinHandle<Result<Bytes, common_error::DaftError>>;
            let mut group_handles: Vec<(FetchHandle, u64, Vec<LeafRange>)> =
                Vec::with_capacity(groups.len());
            for (group_start, group_end, members) in groups {
                let io_client = io_client.clone();
                let io_stats = io_stats.clone();
                let uri = uri.clone();
                let range = group_start as usize..group_end as usize;
                let handle = tokio::spawn(async move {
                    let get_result = io_client
                        .single_url_get(
                            uri,
                            Some(daft_io::range::GetRange::Bounded(range)),
                            io_stats,
                        )
                        .await
                        .map_err(|e| {
                            common_error::DaftError::IoError(std::io::Error::other(e.to_string()))
                        })?;
                    get_result.bytes().await.map_err(|e| {
                        common_error::DaftError::IoError(std::io::Error::other(e.to_string()))
                    })
                });
                group_handles.push((handle, group_start, members));
            }

            // Assemble: await each piece's bytes, slice per-leaf O(1). Splits
            // respect chunk boundaries, so each leaf is in exactly one piece.
            let num_cols = active_col_indices.len();
            let fut: futures::future::BoxFuture<
                'static,
                Result<Arc<RgBytesMap>, Arc<common_error::DaftError>>,
            > = async move {
                let mut rg_map: RgBytesMap = std::collections::HashMap::with_capacity(num_cols);
                for (handle, group_start, members) in group_handles {
                    let group_bytes = match handle.await {
                        Ok(Ok(b)) => b,
                        Ok(Err(e)) => return Err(Arc::new(e)),
                        Err(join_err) => {
                            return Err(Arc::new(common_error::DaftError::ValueError(format!(
                                "byte-range fetch task panicked: {}",
                                join_err
                            ))));
                        }
                    };
                    for (col_idx, start, len) in members {
                        let local_start = (start - group_start) as usize;
                        let local_end = local_start + len as usize;
                        let slice = group_bytes.slice(local_start..local_end);
                        rg_map.insert(col_idx, (start, slice));
                    }
                }
                Ok(Arc::new(rg_map))
            }
            .boxed();
            rg_fetches.insert(rg_idx, fut.shared());
        }

        Self {
            rg_fetches,
            file_len,
        }
    }

    fn empty(file_len: u64) -> Self {
        Self {
            rg_fetches: std::collections::HashMap::new(),
            file_len,
        }
    }

    /// Coalesce sorted (leaf, start, len) tuples into ≤MAX_COALESCE_GAP-bounded
    /// runs, then split runs larger than SPLIT_THRESHOLD at chunk boundaries
    /// into pieces ≲ MAX_REQUEST_SIZE. Each leaf ends up in exactly one piece.
    fn coalesce_and_split(leaf_ranges: Vec<LeafRange>) -> Vec<RangeGroup> {
        let mut groups = coalesce_ranges(leaf_ranges, Self::MAX_COALESCE_GAP);
        let mut split_groups: Vec<RangeGroup> = Vec::with_capacity(groups.len());
        for (group_start, group_end, mut members) in groups.drain(..) {
            let group_len = group_end - group_start;
            if group_len <= Self::SPLIT_THRESHOLD {
                split_groups.push((group_start, group_end, members));
                continue;
            }
            members.sort_by_key(|&(_, s, _)| s);
            let mut piece_start = group_start;
            let mut piece_members: Vec<LeafRange> = Vec::new();
            let mut piece_end = piece_start;
            for entry in members {
                let entry_end = entry.1 + entry.2;
                let would_be_size = entry_end - piece_start;
                if !piece_members.is_empty() && would_be_size > Self::MAX_REQUEST_SIZE {
                    split_groups.push((piece_start, piece_end, std::mem::take(&mut piece_members)));
                    piece_start = entry.1;
                }
                piece_end = entry_end;
                piece_members.push(entry);
            }
            if !piece_members.is_empty() {
                split_groups.push((piece_start, piece_end, piece_members));
            }
        }
        split_groups
    }

    async fn read_rg_chunks(
        &self,
        rg_idx: usize,
        leaves: &[usize],
    ) -> DaftResult<std::collections::HashMap<usize, OffsetBytes>> {
        let fut = self.rg_fetches.get(&rg_idx).cloned().ok_or_else(|| {
            common_error::DaftError::ValueError(format!(
                "RemoteChunkSource: no pre-spawned fetch for rg={}",
                rg_idx
            ))
        })?;
        // DaftError isn't Clone, so the Shared future's err is Arc<DaftError>;
        // wrap the inner Display in a fresh ValueError for callers.
        let rg_bytes = fut
            .await
            .map_err(|e| common_error::DaftError::ValueError(format!("{}", e)))?;
        let mut out = std::collections::HashMap::with_capacity(leaves.len());
        for &leaf in leaves {
            let (base, bytes) = rg_bytes.get(&leaf).ok_or_else(|| {
                common_error::DaftError::ValueError(format!(
                    "RemoteChunkSource: chunk not pre-fetched for rg={}, leaf={}",
                    rg_idx, leaf
                ))
            })?;
            out.insert(
                leaf,
                OffsetBytes {
                    base: *base,
                    file_len: self.file_len,
                    bytes: bytes.clone(),
                },
            );
        }
        Ok(out)
    }
}

/// Open a local parquet file and load its metadata via positioned reads.
/// Returns the open File handle (shared across decode tasks via Arc) and the
/// file's total length. No mmap — each decode task does its own `pread` for
/// just the column-chunk bytes it owns (one chunk in memory at a time, not
/// the whole file).
pub(super) async fn open_local_file(
    path: &str,
) -> DaftResult<(Arc<std::fs::File>, u64, ArrowReaderMetadata)> {
    let path_owned = path.to_string();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path_owned).map_err(|e| {
            common_error::DaftError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to open '{}': {}", path_owned, e),
            ))
        })?;
        let file_len = file
            .metadata()
            .map_err(|e| {
                common_error::DaftError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to stat '{}': {}", path_owned, e),
                ))
            })?
            .len();
        // parquet implements ChunkReader for &File via try_clone+seek+read; the
        // footer load only happens once, so the per-call cost is fine here.
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(parquet_err)?;
        DaftResult::Ok((Arc::new(file), file_len, meta))
    })
    .await?
}

/// Remote fetcher. Reads the parquet footer, prunes row groups by
/// limit/offset, applies any Iceberg field-id mapping, then constructs a
/// `RemoteChunkSource` whose `from_ranged` constructor spawns per-coalesced-
/// range fetch tasks that run in the background while the caller proceeds
/// with decode setup. Big column chunks get split into ~16MB parallel reads
/// because a single sequential GET is bandwidth-limited (~500 MB/s on S3)
/// while 16×16MB parallel ranges aggregate to 1+ GB/s.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fetch_remote_chunk_source(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    field_id_mapping: Option<&BTreeMap<i32, Field>>,
) -> DaftResult<(ChunkSource, ArrowReaderMetadata, Option<Vec<i64>>)> {
    // Fetch metadata + file size. Two round-trips at worst (suffix-GET for
    // the footer + HEAD for the size); on backends that support suffix
    // ranges we still need the absolute file size for constructing the
    // bounded byte-range GETs below.
    let (parquet_metadata_res, file_size_res) = futures::future::join(
        crate::metadata::read_parquet_metadata(
            uri,
            None,
            io_client.clone(),
            io_stats.clone(),
            None,
            None,
        ),
        io_client.single_url_get_size(uri.to_string(), io_stats.clone()),
    )
    .await;
    let mut parquet_metadata = parquet_metadata_res.map_err(common_error::DaftError::from)?;
    let file_size = file_size_res
        .map_err(|e| common_error::DaftError::IoError(std::io::Error::other(e.to_string())))?;

    // Apply Iceberg field-id rewriting BEFORE filtering active leaves by
    // column name. Otherwise the prefetch matches pre-rename names against
    // post-rename user-supplied column names, fetches zero leaves, and the
    // decoder errors with "chunk not pre-fetched for rg=X, leaf=Y".
    if let Some(mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }

    let schema_descr = parquet_metadata.file_metadata().schema_descr();
    let num_cols_total = schema_descr.num_columns();
    let active_col_indices: Vec<usize> = match columns {
        None => (0..num_cols_total).collect(),
        Some(names) => {
            let want: std::collections::HashSet<&str> = names.iter().copied().collect();
            (0..num_cols_total)
                .filter(|&i| {
                    schema_descr
                        .column(i)
                        .path()
                        .parts()
                        .first()
                        .map(|n| want.contains(n.as_str()))
                        .unwrap_or(false)
                })
                .collect()
        }
    };
    let num_rgs = parquet_metadata.num_row_groups();
    let candidate_rgs: Vec<usize> = match row_groups {
        None => (0..num_rgs).collect(),
        Some(rgs) => rgs.iter().map(|&i| i as usize).collect(),
    };

    // Prune RGs by start_offset + num_rows.
    // The pruned set is returned in the tuple so the caller can pass it as the
    // `row_groups` parameter to the downstream stream — otherwise the stream
    // would try to decode RGs whose bytes we never fetched.
    let offset = start_offset.unwrap_or(0);
    let mut active_rg_indices: Vec<usize> = Vec::with_capacity(candidate_rgs.len());
    let mut cumulative = 0usize;
    let mut rows_remaining: i64 = num_rows.map(|n| n as i64).unwrap_or(i64::MAX);
    for &rg_idx in &candidate_rgs {
        let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
        let rg_start = cumulative;
        let rg_end = cumulative + rg_rows;
        cumulative = rg_end;
        if rg_end <= offset {
            continue;
        }
        if rows_remaining <= 0 {
            break;
        }
        active_rg_indices.push(rg_idx);
        let contrib = if rg_start < offset {
            rg_end - offset
        } else {
            rg_rows
        };
        rows_remaining = rows_remaining.saturating_sub(contrib as i64);
    }

    let override_rgs: Option<Vec<i64>> =
        Some(active_rg_indices.iter().map(|&i| i as i64).collect());

    if active_rg_indices.is_empty() {
        // Nothing to fetch (e.g. limit=0 or offset >= total rows).
        let meta = ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new())
            .map_err(parquet_err)?;
        return Ok((
            ChunkSource::Remote(RemoteChunkSource::empty(file_size as u64)),
            meta,
            override_rgs,
        ));
    }

    let chunk_source_enum = ChunkSource::Remote(RemoteChunkSource::from_ranged(
        parquet_metadata.clone(),
        file_size,
        &active_col_indices,
        &active_rg_indices,
        io_client,
        io_stats,
        uri.to_string(),
    ));
    let meta = ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new())
        .map_err(parquet_err)?;
    Ok((chunk_source_enum, meta, override_rgs))
}
