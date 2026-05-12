//! Server-side async coalescer for Flight shuffle partition files.
//!
//! Map tasks register `PartitionCache` entries one-per-(task, partition); a read
//! of partition P walks every entry for P and emits one file-range read per
//! entry. The cost asymmetry across writer modes:
//!
//! - `multi_file`: M files per partition, M opens per partition read, and each
//!   source file holds *exactly one partition* — coalescing fully empties each
//!   source and lets us unlink it. This is the natural target: clean disk
//!   reduction (M*N → N files) and clean read-side reduction (M opens → 1).
//! - `oneshot`: M files total (each with N partitions packed by byte range).
//!   Coalescing partition P pulls P's range from each source into a new file,
//!   so read-side opens drop from M to 1 per partition; but the sources still
//!   reference the other N-1 partitions, so we cannot unlink them. Costs an
//!   extra copy of the partition's bytes, in exchange for a sequential layout
//!   on the read side.
//! - `append`: already 1 file per partition with M ranges in a shared file.
//!   Coalescing rewrites those M ranges as one contiguous block. Marginal on
//!   NVMe, helpful on EBS; the most expendable target.
//!
//! Trigger: `register_shuffle_partitions` dispatches this on every map-task
//! finalize. For each `(shuffle_id, partition_idx)` group whose entry count and
//! total bytes exceed thresholds and that isn't already in flight, we plan +
//! execute a rewrite on a bounded-concurrency permit pool.
//!
//! Output entries stay 1:1 with sources — `partition_ref_id`s are stable for any
//! in-flight tickets. Entries' `byte_ranges` are rewritten to point at slices of
//! the new combined file; `get_shuffle_file_specs` already groups by `file_path`,
//! so the read side naturally collapses to one open + N seeks.
//!
//! Fault model:
//! - For multi_file sources (each source fully consumed by one coalesce),
//!   we unlink the source after the cache swap. POSIX semantics keep any
//!   already-open reader's handle valid through the unlink.
//! - For range-form sources (oneshot, append — sources may still be referenced
//!   by other partitions or by entries we're not rewriting), we never unlink.
//!   GC of those happens at shuffle-cleanup time elsewhere.

use std::{
    collections::{HashMap, HashSet},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_schema::schema::SchemaRef;
use tokio::sync::{Mutex, Semaphore};

use crate::shuffle_cache::PartitionCache;

/// IPC EOS marker length appended by `StreamWriter::finish()`: continuation
/// marker (4 bytes, 0xFFFFFFFF LE) + meta_len 0 (4 bytes). The reader strips
/// this so we must too when extracting body bytes from a whole-file entry.
const IPC_EOS_LEN: u64 = 8;

/// Tunables for the coalescer. All defaults conservative — coalesce only when
/// there are clearly many small entries.
#[derive(Clone, Copy, Debug)]
pub struct CoalescerConfig {
    /// Minimum number of cache entries for a `(shuffle_id, partition_idx)` group
    /// before coalescing fires. Below this, the per-entry read cost is bounded
    /// regardless and rewriting is wasted I/O.
    pub min_entries: usize,
    /// Minimum total bytes across the group's entries before coalescing fires.
    /// Below this, the entries are small enough that any open-cost win is rounding.
    pub min_bytes: u64,
    /// Maximum number of coalesce tasks running concurrently across all partitions.
    /// Keeps the coalescer from saturating disk bandwidth that map tasks need.
    pub max_concurrent: usize,
}

impl Default for CoalescerConfig {
    fn default() -> Self {
        Self {
            min_entries: 16,
            min_bytes: 64 * 1024 * 1024,
            max_concurrent: 2,
        }
    }
}

/// Inspects a snapshot of registered partition caches and returns the
/// `(shuffle_id, partition_idx)` groups that meet the configured thresholds and
/// aren't already being coalesced. Pure function over the snapshot — no I/O.
pub fn pick_candidates<'a>(
    entries: impl Iterator<Item = (u64, u64, &'a PartitionCache)>,
    cfg: &CoalescerConfig,
    in_flight: &HashSet<(u64, usize)>,
) -> Vec<(u64, usize)> {
    let mut by_group: HashMap<(u64, usize), (usize, u64)> = HashMap::new();
    for (shuffle_id, ref_id, cache) in entries {
        if cache.file_paths.is_empty() {
            continue;
        }
        let partition_idx = (ref_id & 0xFFFF_FFFF) as usize;
        let key = (shuffle_id, partition_idx);
        if in_flight.contains(&key) {
            continue;
        }
        let entry_bytes: u64 = cache.bytes_per_file.iter().map(|b| *b as u64).sum();
        let agg = by_group.entry(key).or_insert((0, 0));
        agg.0 += 1;
        agg.1 += entry_bytes;
    }
    by_group
        .into_iter()
        .filter(|(_, (count, bytes))| *count >= cfg.min_entries && *bytes >= cfg.min_bytes)
        .map(|(key, _)| key)
        .collect()
}

/// Shared state for the server's coalescer. Held by `ShuffleFlightServer`.
pub struct CoalescerState {
    pub cfg: CoalescerConfig,
    pub in_flight: Mutex<HashSet<(u64, usize)>>,
    pub permits: Arc<Semaphore>,
    /// Per-(shuffle_id, partition_idx) monotonic sequence to suffix coalesced
    /// file names — guarantees uniqueness if the same group gets coalesced more
    /// than once across separate registers.
    pub seq: Mutex<HashMap<(u64, usize), u64>>,
}

impl CoalescerState {
    pub fn new(cfg: CoalescerConfig) -> Self {
        let permits = Arc::new(Semaphore::new(cfg.max_concurrent));
        Self {
            cfg,
            in_flight: Mutex::new(HashSet::new()),
            permits,
            seq: Mutex::new(HashMap::new()),
        }
    }
}

/// Plan for one coalesce operation: which entries (keyed by ref_id) get rewritten,
/// and where to write the consolidated bytes.
pub struct CoalescePlan {
    pub shuffle_id: u64,
    pub partition_idx: usize,
    pub entries: Vec<(u64, PartitionCache)>,
    pub out_path: String,
}

/// Build a coalesce plan from a snapshot of the cache. Returns `None` if the
/// group has no usable entries.
pub fn build_plan<'a>(
    shuffle_id: u64,
    partition_idx: usize,
    entries_for_group: impl Iterator<Item = (u64, &'a PartitionCache)>,
    seq: u64,
) -> Option<CoalescePlan> {
    let entries: Vec<(u64, PartitionCache)> = entries_for_group
        .filter(|(_, c)| !c.file_paths.is_empty())
        .map(|(ref_id, c)| (ref_id, c.clone()))
        .collect();
    if entries.is_empty() {
        return None;
    }
    // Pick output dir from the first source file's parent. Works regardless of
    // shuffle_dirs sharding scheme — coalesced output lands alongside its sources.
    let first_path = entries[0].1.file_paths[0].clone();
    let parent = Path::new(&first_path).parent()?.to_string_lossy().to_string();
    let out_path = format!(
        "{}/coalesced_part_{}_{}.arrow",
        parent, partition_idx, seq
    );
    Some(CoalescePlan {
        shuffle_id,
        partition_idx,
        entries,
        out_path,
    })
}

/// Result of executing a plan: updated cache entries to swap in, plus source
/// files that became fully dead (multi_file) and can be unlinked after the swap.
pub struct CoalesceOutcome {
    pub updated_entries: Vec<(u64, PartitionCache)>,
    pub orphaned_sources: Vec<String>,
}

/// Execute the plan: read each source byte range, append to the output file,
/// build updated cache entries that point at the output file. Runs on the
/// blocking IO pool — file copy is pure syscalls, no async benefit.
pub async fn execute_plan(plan: CoalescePlan) -> DaftResult<CoalesceOutcome> {
    get_io_runtime(true)
        .spawn_blocking(move || execute_plan_sync(plan))
        .await?
}

/// Compute the byte length of the IPC schema header for the given schema. The
/// header is what `StreamWriter::try_new_with_options` emits before any batches —
/// continuation + meta_len + metadata padded to 8. For our writers `IpcWriteOptions`
/// is `default()`, so this is deterministic per schema.
fn schema_header_len(schema: &SchemaRef) -> DaftResult<u64> {
    let arrow_schema = schema.to_arrow()?;
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    let opts = IpcWriteOptions::default();
    let _ = StreamWriter::try_new_with_options(&mut buf, &arrow_schema, opts)
        .map_err(|e| DaftError::InternalError(format!("schema header probe: {}", e)))?;
    Ok(buf.len() as u64)
}

fn execute_plan_sync(plan: CoalescePlan) -> DaftResult<CoalesceOutcome> {
    let CoalescePlan {
        shuffle_id: _,
        partition_idx: _,
        entries,
        out_path,
    } = plan;

    let mut out = std::fs::File::create(&out_path).map_err(|e| {
        DaftError::IoError(std::io::Error::new(
            e.kind(),
            format!("coalescer create {}: {}", out_path, e),
        ))
    })?;
    let mut cursor: u64 = 0;
    // 1 MiB copy buffer — amortizes syscalls, stays L2-friendly.
    let mut buf = vec![0u8; 1024 * 1024];

    let mut updated_entries: Vec<(u64, PartitionCache)> = Vec::with_capacity(entries.len());
    let mut orphaned_sources: Vec<String> = Vec::new();

    for (ref_id, cache) in entries {
        // For whole-file entries (multi_file), the body bytes (pure batch
        // messages) live in [header_len, file_size - eos_len). We strip these
        // before writing so the coalesced output stays a valid stream of pure
        // batch messages, matching the on-read contract for range-form caches.
        let header_len = if cache.byte_ranges.is_none() {
            schema_header_len(&cache.schema)?
        } else {
            0
        };

        let mut new_byte_ranges: Vec<(u64, u64)> = Vec::with_capacity(cache.file_paths.len());
        let mut new_file_paths: Vec<String> = Vec::with_capacity(cache.file_paths.len());
        let mut new_bytes_per_file: Vec<usize> = Vec::with_capacity(cache.file_paths.len());

        // Resolve (start, end) per source file from either explicit ranges or
        // whole-file body bounds. Iterate either way through the same copy loop.
        let source_ranges: Vec<(String, u64, u64)> = match &cache.byte_ranges {
            Some(ranges) => cache
                .file_paths
                .iter()
                .zip(ranges.iter())
                .map(|(p, (s, e))| (p.clone(), *s, *e))
                .collect(),
            None => {
                let mut v = Vec::with_capacity(cache.file_paths.len());
                for p in &cache.file_paths {
                    let meta = std::fs::metadata(p).map_err(|e| {
                        DaftError::IoError(std::io::Error::new(
                            e.kind(),
                            format!("coalescer stat {}: {}", p, e),
                        ))
                    })?;
                    let size = meta.len();
                    let body_start = header_len.min(size);
                    let body_end = size.saturating_sub(IPC_EOS_LEN).max(body_start);
                    v.push((p.clone(), body_start, body_end));
                }
                v
            }
        };

        for (src_path, start, end) in &source_ranges {
            let len = end.saturating_sub(*start);
            if len == 0 {
                new_file_paths.push(out_path.clone());
                new_byte_ranges.push((cursor, cursor));
                new_bytes_per_file.push(0);
                continue;
            }
            let mut src = std::fs::File::open(src_path).map_err(|e| {
                DaftError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("coalescer open {}: {}", src_path, e),
                ))
            })?;
            src.seek(SeekFrom::Start(*start))?;
            let entry_start = cursor;
            let mut remaining = len;
            while remaining > 0 {
                let take = remaining.min(buf.len() as u64) as usize;
                src.read_exact(&mut buf[..take])?;
                out.write_all(&buf[..take])?;
                cursor += take as u64;
                remaining -= take as u64;
            }
            new_file_paths.push(out_path.clone());
            new_byte_ranges.push((entry_start, cursor));
            new_bytes_per_file.push(len as usize);
        }

        // Sources are fully dead only when each source's *entire* contents were
        // copied. That's true for whole-file (multi_file) entries by definition:
        // each source held exactly one partition's data, and we just rewrote
        // every cache reference to it. For range-form sources we cannot make
        // this claim — sibling partitions may still reference the same file.
        if cache.byte_ranges.is_none() {
            orphaned_sources.extend(cache.file_paths.iter().cloned());
        }

        updated_entries.push((
            ref_id,
            PartitionCache {
                partition_ref_id: cache.partition_ref_id,
                schema: cache.schema.clone(),
                bytes_per_file: new_bytes_per_file,
                file_paths: new_file_paths,
                num_rows: cache.num_rows,
                size_bytes: cache.size_bytes,
                byte_ranges: Some(new_byte_ranges),
            },
        ));
    }

    out.flush()?;
    drop(out);
    Ok(CoalesceOutcome {
        updated_entries,
        orphaned_sources,
    })
}
