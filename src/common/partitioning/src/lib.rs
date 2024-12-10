use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_error::DaftResult;
use futures::stream::BoxStream;
/// Marker trait for forward referencing a partition
pub trait Partition: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn size_bytes(&self) -> DaftResult<Option<usize>>;
}

pub type PartitionRef = Arc<dyn Partition>;

pub type PartitionId = Arc<str>;
/// A collection of related [`MicroPartition`]'s that can be processed as a single unit.
pub trait PartitionBatch: Send + Sync {
    fn partitions(&self) -> Vec<PartitionRef>;
    fn metadata(&self) -> PartitionMetadata;
    fn into_partition_stream(self: Arc<Self>) -> BoxStream<'static, DaftResult<PartitionRef>>;
}
pub type PartitionBatchRef = Arc<dyn PartitionBatch>;

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
#[typetag::serde(tag = "type")]
pub trait PartitionSet: std::fmt::Debug + Send + Sync {
    /// Merge all micropartitions into a single micropartition
    fn get_merged_partitions(&self) -> DaftResult<PartitionRef>;
    /// Get a preview of the micropartitions
    fn get_preview_partitions(&self, num_rows: usize) -> DaftResult<Vec<PartitionRef>>;
    fn items(&self) -> DaftResult<Vec<(PartitionId, PartitionBatchRef)>>;
    fn values(&self) -> DaftResult<Vec<PartitionBatchRef>> {
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
    fn set_partition(&mut self, idx: PartitionId, part: &dyn PartitionBatch) -> DaftResult<()>;
    /// Get a partition
    fn get_partition(&self, idx: &PartitionId) -> DaftResult<PartitionBatchRef>;

    /// Consume the partition set and return a stream of partitions
    fn into_partition_stream(self: Arc<Self>) -> BoxStream<'static, DaftResult<PartitionRef>>;
}

pub type PartitionSetRef = Arc<dyn PartitionSet>;

pub trait PartitionSetCache: std::fmt::Debug + Send + Sync {
    fn get_partition_set(&self, pset_id: &str) -> Option<PartitionSetRef>;
    fn get_all_partition_sets(&self) -> HashMap<PartitionId, PartitionSetRef>;
    fn put_partition_set(&self, pset_id: PartitionId, pset: PartitionSetRef) -> DaftResult<()>;
    fn rm(&self, pset_id: &str);
    fn clear(&self);
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryPartitionSetCache {
    uuid_to_partition_set: Arc<RwLock<HashMap<PartitionId, PartitionSetRef>>>,
}

impl InMemoryPartitionSetCache {
    pub fn new() -> Self {
        Self {
            uuid_to_partition_set: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl PartitionSetCache for InMemoryPartitionSetCache {
    fn get_partition_set(&self, pset_id: &str) -> Option<PartitionSetRef> {
        let lock = self.uuid_to_partition_set.read().unwrap();
        let map = lock.get(pset_id);
        map.cloned()
    }

    fn get_all_partition_sets(&self) -> HashMap<Arc<str>, PartitionSetRef> {
        let lock = self.uuid_to_partition_set.read().unwrap();
        lock.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn put_partition_set(&self, pset_id: PartitionId, pset: PartitionSetRef) -> DaftResult<()> {
        let mut lock = self.uuid_to_partition_set.write().unwrap();
        lock.insert(pset_id, pset);
        Ok(())
    }

    fn rm(&self, pset_id: &str) {
        let mut lock = self.uuid_to_partition_set.write().unwrap();
        lock.remove(pset_id);
    }

    fn clear(&self) {
        let mut lock = self.uuid_to_partition_set.write().unwrap();
        lock.clear();
    }
}

pub type PartitionSetCacheRef = Arc<dyn PartitionSetCache>;

#[cfg(feature = "python")]
#[derive(Debug, Clone)]
pub struct PyPartitionSet {
    pub inner: pyo3::PyObject,
}

#[cfg(feature = "python")]
#[derive(Debug, Clone)]
pub struct PyPartitionSetCache {
    pub inner: pyo3::PyObject,
}

#[cfg(feature = "python")]
impl PyPartitionSetCache {
    pub fn new(inner: pyo3::PyObject) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "python")]
impl PartitionSetCache for PyPartitionSetCache {
    fn get_partition_set(&self, _pset_id: &str) -> Option<PartitionSetRef> {
        todo!("this is all handled in python world right now")
        // let all_part_sets = self.inner.call0("get_all_partition_sets");
    }

    fn get_all_partition_sets(&self) -> HashMap<Arc<str>, PartitionSetRef> {
        todo!("this is all handled in python world right now")
    }

    fn put_partition_set(&self, _pset_id: PartitionId, _pset: PartitionSetRef) -> DaftResult<()> {
        todo!("this is all handled in python world right now")
    }

    fn rm(&self, _pset_id: &str) {
        todo!("this is all handled in python world right now")
    }

    fn clear(&self) {
        todo!("this is all handled in python world right now")
    }
}
