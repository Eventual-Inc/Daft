//! In-memory cache for shuffle output files.
//!
//! On the read side, M reducers each ask the server for their slice from each input
//! map task's combined-output file. Without this cache, each file is opened M times
//! (once per reducer); with it, the file is read from disk once on first touch and
//! all subsequent reducers slice from the in-memory `Arc<[u8]>`.
//!
//! Concurrent requests for the same path coalesce on a `tokio::sync::OnceCell` — only
//! one disk read happens even under thundering-herd access. FIFO eviction keeps the
//! resident set under a byte cap; entries are dropped once total cached bytes exceed
//! the cap.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::Ordering},
};

use common_error::{DaftError, DaftResult};
use tokio::sync::{Mutex, OnceCell};

/// Process-wide counters for shuffle read-cache effectiveness.
pub mod cache_agg {
    use std::sync::atomic::AtomicU64;
    /// Number of `get` calls served from already-cached bytes (no disk read).
    pub static HITS: AtomicU64 = AtomicU64::new(0);
    /// Number of `get` calls that triggered a disk read (cache miss).
    pub static MISSES: AtomicU64 = AtomicU64::new(0);
    /// Number of `get` calls that bypassed the cache (cap_bytes == 0).
    pub static BYPASS: AtomicU64 = AtomicU64::new(0);
    /// Bytes read from disk on cache misses.
    pub static MISS_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Bytes served from cache (sum across hits).
    pub static HIT_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Number of FIFO evictions performed.
    pub static EVICTIONS: AtomicU64 = AtomicU64::new(0);
    /// Bytes reclaimed by evictions.
    pub static EVICTED_BYTES: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Clone, Copy)]
pub struct CacheAggSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub bypass: u64,
    pub miss_bytes: u64,
    pub hit_bytes: u64,
    pub evictions: u64,
    pub evicted_bytes: u64,
}

pub fn cache_agg_snapshot() -> CacheAggSnapshot {
    CacheAggSnapshot {
        hits: cache_agg::HITS.load(Ordering::Relaxed),
        misses: cache_agg::MISSES.load(Ordering::Relaxed),
        bypass: cache_agg::BYPASS.load(Ordering::Relaxed),
        miss_bytes: cache_agg::MISS_BYTES.load(Ordering::Relaxed),
        hit_bytes: cache_agg::HIT_BYTES.load(Ordering::Relaxed),
        evictions: cache_agg::EVICTIONS.load(Ordering::Relaxed),
        evicted_bytes: cache_agg::EVICTED_BYTES.load(Ordering::Relaxed),
    }
}

/// Default byte cap when not overridden by env. 8 GiB chosen empirically: leaves
/// room on a 64 GiB worker for the daft runtime + page cache while still holding
/// enough hot files to give most reducers a hit. Tune up if shuffle stages spill
/// large files; tune down on memory-constrained nodes.
const DEFAULT_CAP_BYTES: usize = 8 * 1024 * 1024 * 1024;

pub struct FileReadCache {
    cap_bytes: usize,
    state: Arc<Mutex<State>>,
}

struct State {
    /// Path -> OnceCell holding the file bytes (or empty if the read is in flight).
    cells: HashMap<String, Arc<OnceCell<Arc<[u8]>>>>,
    /// FIFO order for eviction; one entry per cell.
    order: VecDeque<String>,
    /// Total accounted bytes across initialized cells. Updated after each successful
    /// init and after each eviction.
    bytes: usize,
}

impl FileReadCache {
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            cap_bytes,
            state: Arc::new(Mutex::new(State {
                cells: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
            })),
        }
    }

    /// Construct from env var `DAFT_SHUFFLE_READ_CACHE_BYTES`. Falls back to
    /// `DEFAULT_CAP_BYTES`. Set to 0 to disable the cache entirely (each get
    /// becomes a direct disk read; nothing is retained).
    pub fn from_env() -> Self {
        let cap = std::env::var("DAFT_SHUFFLE_READ_CACHE_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_CAP_BYTES);
        Self::new(cap)
    }

    pub fn cap_bytes(&self) -> usize {
        self.cap_bytes
    }

    /// Read the entire contents of `path`. Concurrent gets for the same path
    /// coalesce — only one disk read happens, all waiters receive the same
    /// `Arc<[u8]>`.
    pub async fn get(&self, path: &str) -> DaftResult<Arc<[u8]>> {
        if self.cap_bytes == 0 {
            cache_agg::BYPASS.fetch_add(1, Ordering::Relaxed);
            let v = tokio::fs::read(path).await.map_err(DaftError::IoError)?;
            return Ok(Arc::from(v.into_boxed_slice()));
        }

        // Look up or create the cell. New cells push onto the FIFO order so eviction
        // sees them. We do NOT account bytes here — that happens after the init
        // closure runs, so a failed init doesn't leak phantom bytes.
        let (cell, was_initialized) = {
            let mut s = self.state.lock().await;
            match s.cells.get(path) {
                Some(c) => {
                    let initialized = c.initialized();
                    (c.clone(), initialized)
                }
                None => {
                    let c = Arc::new(OnceCell::new());
                    s.cells.insert(path.to_string(), c.clone());
                    s.order.push_back(path.to_string());
                    (c, false)
                }
            }
        };

        let state = self.state.clone();
        let path_owned = path.to_string();
        let cap_bytes = self.cap_bytes;

        let bytes = cell
            .get_or_try_init(|| async move {
                cache_agg::MISSES.fetch_add(1, Ordering::Relaxed);
                let v = tokio::fs::read(&path_owned)
                    .await
                    .map_err(DaftError::IoError)?;
                let bytes: Arc<[u8]> = Arc::from(v.into_boxed_slice());
                cache_agg::MISS_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);

                // Account this entry's bytes and evict FIFO until under cap.
                let mut s = state.lock().await;
                s.bytes = s.bytes.saturating_add(bytes.len());
                while s.bytes > cap_bytes {
                    let Some(oldest) = s.order.front().cloned() else {
                        break;
                    };
                    if oldest == path_owned {
                        // Don't evict the entry we just inserted; that would defeat
                        // the purpose of the caller's read.
                        break;
                    }
                    s.order.pop_front();
                    if let Some(c) = s.cells.remove(&oldest) {
                        if let Some(b) = c.get() {
                            let n = b.len();
                            s.bytes = s.bytes.saturating_sub(n);
                            cache_agg::EVICTIONS.fetch_add(1, Ordering::Relaxed);
                            cache_agg::EVICTED_BYTES.fetch_add(n as u64, Ordering::Relaxed);
                        }
                    }
                }
                Ok::<Arc<[u8]>, DaftError>(bytes)
            })
            .await?
            .clone();

        if was_initialized {
            cache_agg::HITS.fetch_add(1, Ordering::Relaxed);
            cache_agg::HIT_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(bytes)
    }
}

impl Default for FileReadCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAP_BYTES)
    }
}

pub fn log_cache_agg_summary(label: &str) {
    let s = cache_agg_snapshot();
    let mb = 1024.0 * 1024.0;
    let total_reqs = s.hits + s.misses;
    let hit_rate = if total_reqs == 0 {
        0.0
    } else {
        s.hits as f64 / total_reqs as f64
    };
    tracing::info!(
        target: "daft_shuffles::read_cache",
        label = label,
        hits = s.hits,
        misses = s.misses,
        bypass = s.bypass,
        hit_rate = format_args!("{:.3}", hit_rate),
        miss_bytes_mib = format_args!("{:.1}", s.miss_bytes as f64 / mb),
        hit_bytes_mib = format_args!("{:.1}", s.hit_bytes as f64 / mb),
        evictions = s.evictions,
        evicted_bytes_mib = format_args!("{:.1}", s.evicted_bytes as f64 / mb),
        "shuffle read cache summary",
    );
}
