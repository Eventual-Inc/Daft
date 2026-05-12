//! Server-side async coalescer for Flight shuffle partition files.
//!
//! Map tasks register `PartitionCache` entries one-per-(task, partition); a read of
//! partition P walks every entry for P and emits one file-range read per entry. When
//! many small map tasks fan out across many output partitions, this leaves the read
//! side doing N opens / N seeks per partition.
//!
//! This module runs in the background on the Flight server, watches for
//! `(shuffle_id, partition_idx)` groups whose entry count and total bytes cross
//! configured thresholds, and rewrites them: read the source byte ranges, append
//! them to a single new file under the same shuffle dir, and atomically swap each
//! entry to point at its slice of the new file. Cache entries stay 1:1 (so
//! `partition_ref_id`s stay stable for any in-flight tickets); they just all end up
//! pointing at the same physical file, which `get_shuffle_file_specs` already
//! collapses into a single open with multiple ranges.
//!
//! Scope: only `byte_ranges = Some(_)` entries (oneshot + append writers, where
//! the bytes are pure IPC batch messages with no schema header / EOS embedded in
//! the range). Whole-file entries (multi_file writer) are skipped — coalescing
//! them needs IPC header/EOS extraction; that can come later if it matters.
//!
//! Fault model: source files are never deleted by the coalescer. Readers that
//! opened a path before the swap keep using their handle; readers that look up
//! after the swap get the new path. GC of orphaned source files happens at
//! shuffle-cleanup time, not here. Worst case: 2x disk space until cleanup.

use std::{
    collections::{HashMap, HashSet},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use tokio::sync::{Mutex, Semaphore};

use crate::shuffle_cache::PartitionCache;

/// Tunables for the coalescer. All defaults conservative — coalesce only when there
/// are clearly many small entries.
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
    // Aggregate count + bytes per group.
    let mut by_group: HashMap<(u64, usize), (usize, u64)> = HashMap::new();
    for (shuffle_id, ref_id, cache) in entries {
        // Range-form entries only — see module-level note.
        if cache.byte_ranges.is_none() || cache.file_paths.is_empty() {
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
    /// (ref_id, cloned cache) snapshot at planning time. The actual cache may have
    /// new entries appended afterwards; that's fine — we just won't coalesce those
    /// in this pass.
    pub entries: Vec<(u64, PartitionCache)>,
    /// Absolute path of the new combined file.
    pub out_path: String,
}

/// Build a coalesce plan from a snapshot of the cache. Returns `None` if no
/// range-form entries with file paths exist (defensive — `pick_candidates`
/// should have filtered those out already).
pub fn build_plan<'a>(
    shuffle_id: u64,
    partition_idx: usize,
    entries_for_group: impl Iterator<Item = (u64, &'a PartitionCache)>,
    seq: u64,
) -> Option<CoalescePlan> {
    let entries: Vec<(u64, PartitionCache)> = entries_for_group
        .filter(|(_, c)| c.byte_ranges.is_some() && !c.file_paths.is_empty())
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

/// Execute the plan: read each source byte range, append to the output file,
/// build updated cache entries that point at the output file. Runs on the
/// blocking IO pool — file copy is pure syscalls, no async benefit.
///
/// Returns updated `(ref_id, PartitionCache)` pairs ready to swap into the
/// server's `shuffle_partitions` map.
pub async fn execute_plan(plan: CoalescePlan) -> DaftResult<Vec<(u64, PartitionCache)>> {
    get_io_runtime(true)
        .spawn_blocking(move || execute_plan_sync(plan))
        .await?
}

fn execute_plan_sync(plan: CoalescePlan) -> DaftResult<Vec<(u64, PartitionCache)>> {
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
    // 1 MiB copy buffer — large enough to amortize syscalls, small enough to stay L2-friendly.
    let mut buf = vec![0u8; 1024 * 1024];

    let mut updated: Vec<(u64, PartitionCache)> = Vec::with_capacity(entries.len());

    for (ref_id, cache) in entries {
        let ranges = cache
            .byte_ranges
            .as_ref()
            .expect("planner filtered out non-range entries");
        // Each cache may carry multiple files (rare but supported). Walk them in order.
        let mut new_byte_ranges: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        let mut new_file_paths: Vec<String> = Vec::with_capacity(ranges.len());
        let mut new_bytes_per_file: Vec<usize> = Vec::with_capacity(ranges.len());

        for (src_path, (start, end)) in cache.file_paths.iter().zip(ranges.iter()) {
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

        updated.push((
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
    Ok(updated)
}
