use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_error::{DaftError, DaftResult};
use daft_micropartition::{
    partitioning::{PartitionId, PartitionSetCache, PartitionSetRef},
    MicroPartition,
};

/// An in memory cache for partition sets
/// This is a simple in memory cache that stores partition sets in a HashMap.
///
/// TODO: this should drop partitions when the reference count for the partition id drops to 0
///
/// TODO: **This will continue to hold onto partitions indefinitely**
#[derive(Debug, Default, Clone)]
pub struct InMemoryPartitionSetCache {
    uuid_to_partition_set: Arc<RwLock<HashMap<PartitionId, PartitionSetRef<MicroPartition>>>>,
}

impl InMemoryPartitionSetCache {
    pub fn new() -> Self {
        Self {
            uuid_to_partition_set: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl PartitionSetCache<MicroPartition> for InMemoryPartitionSetCache {
    fn get_partition_set(
        &self,
        pset_id: &str,
    ) -> DaftResult<Option<PartitionSetRef<MicroPartition>>> {
        let lock = self
            .uuid_to_partition_set
            .read()
            .map_err(|_| DaftError::InternalError("Failed to acquire read lock".to_string()))?;
        let map = lock.get(pset_id);
        Ok(map.cloned())
    }

    fn get_all_partition_sets(
        &self,
    ) -> DaftResult<HashMap<Arc<str>, PartitionSetRef<MicroPartition>>> {
        let lock = self
            .uuid_to_partition_set
            .read()
            .map_err(|_| DaftError::InternalError("Failed to acquire read lock".to_string()))?;
        Ok(lock.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    fn put_partition_set(&self, pset: PartitionSetRef<MicroPartition>) -> DaftResult<()> {
        let uuid = uuid::Uuid::new_v4().to_string();
        let uuid = Arc::<str>::from(uuid);
        let mut lock = self
            .uuid_to_partition_set
            .write()
            .map_err(|_| DaftError::InternalError("Failed to acquire write lock".to_string()))?;
        lock.insert(uuid, pset);
        Ok(())
    }

    fn rm(&self, pset_id: &str) -> DaftResult<()> {
        let mut lock = self
            .uuid_to_partition_set
            .write()
            .map_err(|_| DaftError::InternalError("Failed to acquire write lock".to_string()))?;
        lock.remove(pset_id);
        Ok(())
    }

    fn clear(&self) -> DaftResult<()> {
        let mut lock = self
            .uuid_to_partition_set
            .write()
            .map_err(|_| DaftError::InternalError("Failed to acquire write lock".to_string()))?;
        lock.clear();
        Ok(())
    }
}
