use std::sync::Arc;

use common_error::DaftResult;
use dashmap::DashMap;
use futures::stream::BoxStream;

type PartitionId = String;
/// A collection of related [`MicroPartition`]'s that can be processed as a single unit.
pub trait PartitionBatch<T>: Send + Sync {
    fn partitions(&self) -> Vec<T>;
    fn metadata(&self) -> PartitionMetadata;
    fn into_partition_stream(self: Arc<Self>) -> BoxStream<'static, DaftResult<T>>;
}
pub type PartitionBatchRef<T> = Arc<dyn PartitionBatch<T>>;

/// ported over from `daft/runners/partitioning.py`
// TODO: port over the rest of the functionality
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub num_rows: usize,
    pub size_bytes: usize,
}

/// a collection of [`T`]
///
/// Since we can have different partition sets such as an in memory, or a distributed partition set, we need to abstract over the partition set.
/// This trait defines the common operations that can be performed on a partition set.
pub trait PartitionSet<T> {
    /// Merge all micropartitions into a single micropartition
    fn get_merged_partitions(&self) -> DaftResult<T>;
    /// Get a preview of the micropartitions
    fn get_preview_partitions(&self, num_rows: usize) -> DaftResult<Vec<T>>;
    fn items(&self) -> DaftResult<Vec<(PartitionId, PartitionBatchRef<T>)>>;
    fn values(&self) -> DaftResult<Vec<PartitionBatchRef<T>>> {
        let items = self.items()?;
        Ok(items.into_iter().map(|(_, mp)| mp).collect())
    }
    /// Number of partitions
    fn num_partitions(&self) -> usize;

    fn len(&self) -> usize;
    /// Check if the partition set is empty
    fn is_empty(&self) -> bool;
    /// Size of the partition set in bytes
    fn size_bytes(&self) -> DaftResult<usize>;
    /// Check if a partition exists
    fn has_partition(&self, idx: &PartitionId) -> bool;
    /// Delete a partition
    fn delete_partition(&mut self, idx: &PartitionId) -> DaftResult<()>;
    /// Set a partition
    fn set_partition(&mut self, idx: PartitionId, part: &dyn PartitionBatch<T>) -> DaftResult<()>;
    /// Get a partition
    fn get_partition(&self, idx: &PartitionId) -> DaftResult<PartitionBatchRef<T>>;
}

use std::collections::HashMap;

pub struct PartitionSetCache<T> {
    uuid_to_partition_set: DashMap<String, Arc<dyn PartitionSet<T>>>,
}

impl<T> PartitionSetCache<T> {
    pub fn new() -> Self {
        Self {
            uuid_to_partition_set: DashMap::new(),
        }
    }

    pub fn get_partition_set(&self, pset_id: &str) -> Option<Arc<dyn PartitionSet<T>>> {
        let map = self.uuid_to_partition_set.get(pset_id);
        map.map(|v| v.to_owned())
    }

    pub fn get_all_partition_sets(&self) -> HashMap<String, Arc<dyn PartitionSet<T>>> {
        self.uuid_to_partition_set
            .iter()
            .map(|v| (v.key().clone(), v.value().to_owned()))
            .collect()
    }

    pub fn put_partition_set(&self, pset: Arc<dyn PartitionSet<T>>) -> DaftResult<()> {
        let pset_id = uuid::Uuid::new_v4().to_string();
        self.uuid_to_partition_set.insert(pset_id, pset);
        Ok(())
    }

    pub fn rm(&self, pset_id: &str) {
        self.uuid_to_partition_set.remove(pset_id);
    }

    pub fn clear(&self) {
        self.uuid_to_partition_set.clear();
    }
}
