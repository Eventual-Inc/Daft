use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common_error::{DaftError, DaftResult};

/// A cached Lance dataset with TTL (Time To Live)
#[derive(Clone)]
struct CachedDataset {
    dataset: Arc<lance::Dataset>,
    created_at: Instant,
    last_accessed: Instant,
    access_count: u64,
}

impl CachedDataset {
    fn new(dataset: lance::Dataset) -> Self {
        let now = Instant::now();
        Self {
            dataset: Arc::new(dataset),
            created_at: now,
            last_accessed: now,
            access_count: 1,
        }
    }

    fn access(&mut self) -> Arc<lance::Dataset> {
        self.last_accessed = Instant::now();
        self.access_count += 1;
        self.dataset.clone()
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    #[allow(dead_code)]
    fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Configuration for the Lance dataset cache
#[derive(Debug, Clone)]
pub struct LanceCacheConfig {
    /// Maximum number of datasets to cache
    pub max_size: usize,
    /// Time to live for cached datasets
    pub ttl: Duration,
    /// Enable/disable the cache
    pub enabled: bool,
}

impl Default for LanceCacheConfig {
    fn default() -> Self {
        Self {
            max_size: 100,
            ttl: Duration::from_secs(300), // 5 minutes
            enabled: true,
        }
    }
}

impl LanceCacheConfig {
    pub fn new(max_size: usize, ttl_seconds: u64) -> Self {
        Self {
            max_size,
            ttl: Duration::from_secs(ttl_seconds),
            enabled: true,
        }
    }

    pub fn disabled() -> Self {
        Self {
            max_size: 0,
            ttl: Duration::from_secs(0),
            enabled: false,
        }
    }
}

/// Thread-safe LRU cache for Lance datasets with TTL support
pub struct LanceDatasetCache {
    cache: Arc<Mutex<HashMap<String, CachedDataset>>>,
    config: LanceCacheConfig,
    stats: Arc<Mutex<CacheStats>>,
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub expired: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f64 / (self.hits + self.misses) as f64
        }
    }
}

impl LanceDatasetCache {
    /// Create a new Lance dataset cache with the given configuration
    pub fn new(config: LanceCacheConfig) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            config,
            stats: Arc::new(Mutex::new(CacheStats::default())),
        }
    }

    /// Get a dataset from cache or open it if not cached
    pub async fn get_or_open(&self, uri: &str) -> DaftResult<Arc<lance::Dataset>> {
        if !self.config.enabled {
            // Cache is disabled, open dataset directly
            let dataset = lance::Dataset::open(uri).await.map_err(|e| {
                DaftError::External(
                    format!("Failed to open Lance dataset at {}: {}", uri, e).into(),
                )
            })?;
            return Ok(Arc::new(dataset));
        }

        // Try to get from cache first
        if let Some(dataset) = self.get_from_cache(uri) {
            return Ok(dataset);
        }

        // Not in cache, open the dataset
        let dataset = lance::Dataset::open(uri).await.map_err(|e| {
            DaftError::External(format!("Failed to open Lance dataset at {}: {}", uri, e).into())
        })?;

        // Store in cache
        self.put_in_cache(uri.to_string(), dataset.clone());

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.misses += 1;
        }

        Ok(Arc::new(dataset))
    }

    /// Get dataset from cache if available and not expired
    fn get_from_cache(&self, uri: &str) -> Option<Arc<lance::Dataset>> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(cached) = cache.get_mut(uri) {
            // Check if expired
            if cached.is_expired(self.config.ttl) {
                cache.remove(uri);

                // Update stats
                drop(cache);
                let mut stats = self.stats.lock().unwrap();
                stats.expired += 1;
                stats.misses += 1;

                return None;
            }

            // Update access time and return dataset
            let dataset = cached.access();

            // Update stats
            drop(cache);
            let mut stats = self.stats.lock().unwrap();
            stats.hits += 1;

            return Some(dataset);
        }

        None
    }

    /// Put dataset in cache with LRU eviction
    fn put_in_cache(&self, uri: String, dataset: lance::Dataset) {
        let mut cache = self.cache.lock().unwrap();

        // If cache is at capacity, evict least recently used item
        if cache.len() >= self.config.max_size {
            self.evict_lru(&mut cache);
        }

        // Insert new dataset
        cache.insert(uri, CachedDataset::new(dataset));
    }

    /// Evict the least recently used dataset from cache
    fn evict_lru(&self, cache: &mut HashMap<String, CachedDataset>) {
        if cache.is_empty() {
            return;
        }

        // Find the least recently used item
        let mut lru_key: Option<String> = None;
        let mut lru_time = Instant::now();

        for (key, cached) in cache.iter() {
            if lru_key.is_none() || cached.last_accessed < lru_time {
                lru_key = Some(key.clone());
                lru_time = cached.last_accessed;
            }
        }

        // Remove the LRU item
        if let Some(key) = lru_key {
            cache.remove(&key);

            // Update stats
            let mut stats = self.stats.lock().unwrap();
            stats.evictions += 1;
        }
    }

    /// Clear all cached datasets
    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let stats = self.stats.lock().unwrap();
        CacheStats {
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            expired: stats.expired,
        }
    }

    /// Get current cache size
    pub fn size(&self) -> usize {
        let cache = self.cache.lock().unwrap();
        cache.len()
    }

    /// Clean up expired entries
    pub fn cleanup_expired(&self) {
        let mut cache = self.cache.lock().unwrap();
        let mut expired_keys = Vec::new();

        for (key, cached) in cache.iter() {
            if cached.is_expired(self.config.ttl) {
                expired_keys.push(key.clone());
            }
        }

        let expired_count = expired_keys.len();
        for key in expired_keys {
            cache.remove(&key);
        }

        if expired_count > 0 {
            let mut stats = self.stats.lock().unwrap();
            stats.expired += expired_count as u64;
        }
    }

    /// Get cache configuration
    pub fn config(&self) -> &LanceCacheConfig {
        &self.config
    }
}

/// Implementation of Default trait for LanceDatasetCache
impl Default for LanceDatasetCache {
    fn default() -> Self {
        Self::new(LanceCacheConfig::default())
    }
}

/// Global cache instance
static mut GLOBAL_CACHE: Option<LanceDatasetCache> = None;
static CACHE_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global cache with configuration
pub fn init_global_cache(config: LanceCacheConfig) {
    CACHE_INIT.call_once(|| unsafe {
        GLOBAL_CACHE = Some(LanceDatasetCache::new(config));
    });
}

/// Get the global cache instance
pub fn get_global_cache() -> &'static LanceDatasetCache {
    CACHE_INIT.call_once(|| unsafe {
        GLOBAL_CACHE = Some(LanceDatasetCache::default());
    });

    #[allow(static_mut_refs)]
    unsafe {
        GLOBAL_CACHE.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn test_cache_config() {
        let config = LanceCacheConfig::new(50, 120);
        assert_eq!(config.max_size, 50);
        assert_eq!(config.ttl, Duration::from_secs(120));
        assert!(config.enabled);

        let disabled = LanceCacheConfig::disabled();
        assert!(!disabled.enabled);
    }

    #[test]
    fn test_cache_stats() {
        let stats = CacheStats {
            hits: 80,
            misses: 20,
            evictions: 5,
            expired: 2,
        };
        assert_eq!(stats.hit_rate(), 0.8);

        let empty_stats = CacheStats::default();
        assert_eq!(empty_stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_cached_dataset() {
        // This test would require a mock Lance dataset
        // For now, we just test the basic structure
        let config = LanceCacheConfig::new(10, 60);
        let cache = LanceDatasetCache::new(config);

        assert_eq!(cache.size(), 0);
        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }
}
