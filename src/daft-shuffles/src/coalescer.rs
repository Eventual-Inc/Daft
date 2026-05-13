//! Plan + execute helpers for the seal-time consolidation of Flight shuffle
//! partition files.
//!
//! Map tasks register `PartitionCache` entries one-per-(task, partition). After
//! the producer stage drains, the orchestrator calls `seal_shuffle` on each
//! participating Flight server; that path uses `build_plan` + `execute_plan` to
//! rewrite the per-task byte ranges of each `(shuffle_id, partition_idx)` group
//! into a single combined file, then swaps the cache entries in place.
//!
//! Asymmetry across writer modes:
//! - `multi_file`: each source file holds exactly one partition — fully empty
//!   after seal, so seal can unlink the source. Read-side opens M→1.
//! - `oneshot`: source files hold all partitions; seal pulls each partition's
//!   range into a new file but leaves the source intact (other partitions still
//!   reference it). Read-side opens M→1; disk doubles until shuffle cleanup.
//! - `append`: already 1 file per partition with M ranges; seal rewrites those
//!   ranges contiguously. Marginal on NVMe, useful on EBS.
//!
//! Output entries stay 1:1 with sources — `partition_ref_id`s are stable so any
//! in-flight read ticket keeps resolving. Entries' `byte_ranges` are rewritten
//! to point at slices of the new combined file; `get_shuffle_file_specs` already
//! groups by `file_path`, so the read side collapses to one open + N seeks.
//!
//! Source-cleanup model:
//! - For multi_file sources (each source fully consumed by one coalesce),
//!   seal unlinks the source after the cache swap. POSIX semantics keep any
//!   already-open reader's handle valid through the unlink.
//! - For range-form sources (oneshot, append — siblings may still reference
//!   the file), we never unlink here. GC happens at shuffle-cleanup elsewhere.

use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_schema::schema::SchemaRef;

use crate::shuffle_cache::PartitionCache;

/// IPC EOS marker length appended by `StreamWriter::finish()`: continuation
/// marker (4 bytes, 0xFFFFFFFF LE) + meta_len 0 (4 bytes). The reader strips
/// this so we must too when extracting body bytes from a whole-file entry.
const IPC_EOS_LEN: u64 = 8;

/// Tunables for seal-time consolidation.
#[derive(Clone, Copy, Debug)]
pub struct SealConfig {
    /// Maximum number of coalesce tasks running concurrently inside one seal call.
    /// Keeps total disk I/O bounded; seal is single-shot per shuffle so this only
    /// gates within-shuffle parallelism, not cross-shuffle.
    pub max_concurrent: usize,
}

impl Default for SealConfig {
    fn default() -> Self {
        Self { max_concurrent: 2 }
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
pub async fn execute_plan(plan: CoalescePlan, _cfg: SealConfig) -> DaftResult<CoalesceOutcome> {
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
    // One shared slot for `out_path` — every updated entry references the same
    // newly-created coalesced file, so they should all open it via one cache.
    let out_slot: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<std::fs::File>>> =
        std::sync::Arc::new(std::sync::OnceLock::new());

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

        let new_file_slots = new_file_paths.iter().map(|_| out_slot.clone()).collect();
        updated_entries.push((
            ref_id,
            PartitionCache {
                partition_ref_id: cache.partition_ref_id,
                schema: cache.schema.clone(),
                bytes_per_file: new_bytes_per_file,
                file_paths: new_file_paths,
                file_slots: new_file_slots,
                num_rows: cache.num_rows,
                size_bytes: cache.size_bytes,
                byte_ranges: Some(new_byte_ranges),
                // Seal-time coalescer rewrites paths within partitions but does not
                // merge partitions into shared IPC messages, so no row slicing needed.
                row_ranges: None,
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

