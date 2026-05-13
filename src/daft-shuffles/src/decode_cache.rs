//! Process-wide cache of decoded arrow batches keyed by (file_path, byte_range).
//!
//! Why: with shuffle data stored as IPC bytes on disk, every reducer requesting
//! a partition has to read the file and IPC-decode it. When multiple readers
//! converge on the same byte range — either because the same partition is read
//! by multiple paths (local + remote serve), or because future writers pack
//! multiple partitions into shared IPC messages — they all pay the same decode
//! cost. This cache stores the decoded `Vec<arrow::RecordBatch>` so the second
//! and later readers slice an `Arc` instead of re-decoding.
//!
//! Fault tolerance: the cache is opaque transient state. Disk file is the
//! source of truth. Cache eviction or process restart just means the next
//! reader pays decode once and re-populates.
//!
//! Concurrency: read-heavy via `DashMap`. Two concurrent misses on the same
//! key may both decode (rare; the loser's insert is dropped via `entry()`).
//! Eviction is FIFO under a separate `Mutex<VecDeque>` — adequate for the
//! "burst fan-in during reduce phase" access pattern.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow_array::RecordBatch;
use dashmap::{DashMap, mapref::entry::Entry};

pub type CacheKey = (String, u64, u64);
pub type CachedBatches = Vec<RecordBatch>;

/// Returns `DAFT_SHUFFLE_DECODE_CACHE_BYTES` env var or 2 GiB default.
/// Set to 0 to disable the cache entirely (every read decodes).
fn configured_max_bytes() -> usize {
    std::env::var("DAFT_SHUFFLE_DECODE_CACHE_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2 * 1024 * 1024 * 1024)
}

pub struct DecodeCache {
    entries: DashMap<CacheKey, Arc<CachedBatches>>,
    /// FIFO insertion order — front is oldest. Carries the byte weight so eviction
    /// doesn't need to re-walk entries.
    eviction: Mutex<VecDeque<(CacheKey, usize)>>,
    total_bytes: AtomicUsize,
    max_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    bypassed: AtomicU64,
}

impl DecodeCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: DashMap::new(),
            eviction: Mutex::new(VecDeque::new()),
            total_bytes: AtomicUsize::new(0),
            max_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            bypassed: AtomicU64::new(0),
        }
    }

    /// Lookup. Returns `None` if cache is disabled (max_bytes == 0).
    pub fn get(&self, key: &CacheKey) -> Option<Arc<CachedBatches>> {
        if self.max_bytes == 0 {
            self.bypassed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        match self.entries.get(key) {
            Some(e) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(e.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert. No-op when caching disabled or when the entry size alone exceeds
    /// the whole budget (would just trigger immediate self-eviction).
    pub fn put(&self, key: CacheKey, batches: Arc<CachedBatches>, bytes: usize) {
        if self.max_bytes == 0 || bytes > self.max_bytes {
            return;
        }
        match self.entries.entry(key.clone()) {
            // Already present (another concurrent miss inserted first). Drop ours.
            Entry::Occupied(_) => return,
            Entry::Vacant(e) => {
                e.insert(batches);
            }
        }
        self.eviction.lock().expect("eviction mutex poisoned").push_back((key, bytes));
        let new_total = self.total_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if new_total > self.max_bytes {
            self.evict_until_under_budget();
        }
    }

    fn evict_until_under_budget(&self) {
        let mut eviction = self.eviction.lock().expect("eviction mutex poisoned");
        while self.total_bytes.load(Ordering::Relaxed) > self.max_bytes {
            let Some((key, bytes)) = eviction.pop_front() else {
                break;
            };
            // Only adjust the byte counter if we actually removed (in case
            // somebody concurrently removed via another mechanism).
            if self.entries.remove(&key).is_some() {
                self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn stats(&self) -> DecodeCacheStats {
        DecodeCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            bypassed: self.bypassed.load(Ordering::Relaxed),
            resident_bytes: self.total_bytes.load(Ordering::Relaxed),
            max_bytes: self.max_bytes,
            entries: self.entries.len() as u64,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub bypassed: u64,
    pub resident_bytes: usize,
    pub max_bytes: usize,
    pub entries: u64,
}

static GLOBAL_CACHE: OnceLock<Arc<DecodeCache>> = OnceLock::new();

/// Process-wide singleton. Lazy-initialized on first use.
pub fn global() -> &'static Arc<DecodeCache> {
    GLOBAL_CACHE.get_or_init(|| Arc::new(DecodeCache::new(configured_max_bytes())))
}

/// Estimate the resident memory cost of a vec of arrow batches. Sums column
/// `get_buffer_memory_size` across each batch. Used as the cache-eviction
/// weight; needn't be exact, just monotone in real footprint.
pub fn estimate_batches_bytes(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(|b| {
            b.columns()
                .iter()
                .map(|c| c.get_buffer_memory_size())
                .sum::<usize>()
        })
        .sum()
}

pub fn log_summary(label: &str) {
    let s = global().stats();
    let mb = 1024.0 * 1024.0;
    let lookups = s.hits + s.misses;
    let hit_rate = if lookups == 0 { 0.0 } else { s.hits as f64 / lookups as f64 };
    tracing::info!(
        target: "daft_shuffles::decode_cache",
        label = label,
        hits = s.hits,
        misses = s.misses,
        hit_rate = format_args!("{:.2}", hit_rate),
        evictions = s.evictions,
        bypassed = s.bypassed,
        entries = s.entries,
        resident_mib = format_args!("{:.1}", s.resident_bytes as f64 / mb),
        max_mib = format_args!("{:.1}", s.max_bytes as f64 / mb),
        "shuffle decode cache summary",
    );
}
