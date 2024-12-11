use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_error::DaftResult;
use futures::stream::BoxStream;

/// Common trait interface for dataset partitioning, defined in this shared crate to avoid circular dependencies.
/// Acts as a forward reference for concrete partition implementations. _(Specifically the `MicroPartition` type defined in `daft-micropartition`)_
pub trait Partition: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn size_bytes(&self) -> DaftResult<Option<usize>>;
}

/// An Arc'd reference to a [`Partition`]
pub type PartitionRef = Arc<dyn Partition>;

/// Key used to identify a partition
pub type PartitionId = Arc<str>;

/// A collection of related [`Partitions`]'s that can be processed as a single unit.
/// All partitions in a batch should have the same schema.
pub trait PartitionBatch<T: Partition>: std::fmt::Debug + Send + Sync {
    fn partitions(&self) -> Vec<Arc<T>>;
    fn metadata(&self) -> PartitionMetadata;
    /// consume the partition batch and return a stream of partitions
    fn into_partition_stream(self) -> BoxStream<'static, DaftResult<Arc<T>>>;
}

/// An Arc'd reference to a [`PartitionBatch`]
pub type PartitionBatchRef<T> = Arc<dyn PartitionBatch<T>>;

/// ported over from `daft/runners/partitioning.py`
// TODO: port over the rest of the functionality
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub num_rows: usize,
    pub size_bytes: usize,
}

/// A partition set is a collection of partition batches.
/// It is up to the implementation to decide how to store and manage the partition batches.
/// For example, an in memory partition set could likely be stored as `HashMap<PartitionId, PartitionBatchRef<T>>`.
pub trait PartitionSet<T: Partition>: std::fmt::Debug + Send + Sync {
    /// Merge all micropartitions into a single micropartition
    fn get_merged_partitions(&self) -> DaftResult<PartitionRef>;
    /// Get a preview of the micropartitions
    fn get_preview_partitions(&self, num_rows: usize) -> DaftResult<PartitionBatchRef<T>>;

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

    /// Consume the partition set and return a stream of partitions
    fn into_partition_stream(self: Arc<Self>) -> BoxStream<'static, DaftResult<Arc<T>>>;
}

// An in memory partition set
#[derive(Debug, Default)]
pub struct InMemoryPartitionSet<T: Partition> {
    pub partitions: HashMap<PartitionId, PartitionBatchRef<T>>,
}

pub type PartitionSetRef<T> = Arc<dyn PartitionSet<T>>;

/// A PartitionSetCache is a cache for partition sets.
///
/// A simple in memory cache is provided by `InMemoryPartitionSetCache`.
///
///
/// Implementations can provide more sophisticated caching strategies.
/// It is important to note that the methods do not take `&mut self` but instead take `&self`.
/// So it is up to the implementation to manage any interior mutability.
pub trait PartitionSetCache<T: Partition>: std::fmt::Debug + Send + Sync {
    fn get_partition_set(&self, pset_id: &str) -> Option<PartitionSetRef<T>>;
    fn get_all_partition_sets(&self) -> HashMap<PartitionId, PartitionSetRef<T>>;
    fn put_partition_set(&self, pset_id: PartitionId, pset: PartitionSetRef<T>) -> DaftResult<()>;
    fn rm(&self, pset_id: &str);
    fn clear(&self);
}

#[derive(Debug, Default, Clone)]
/// An in memory cache for partition sets
/// This is a simple in memory cache that stores partition sets in a HashMap.
/// TODO: this should drop partitions when the reference count for the partition id drops to 0
pub struct InMemoryPartitionSetCache<T: Partition> {
    uuid_to_partition_set: Arc<RwLock<HashMap<PartitionId, PartitionSetRef<T>>>>,
}

impl<T: Partition> InMemoryPartitionSetCache<T> {
    pub fn new() -> Self {
        Self {
            uuid_to_partition_set: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<T: Partition> PartitionSetCache<T> for InMemoryPartitionSetCache<T> {
    fn get_partition_set(&self, pset_id: &str) -> Option<PartitionSetRef<T>> {
        let lock = self.uuid_to_partition_set.read().unwrap();
        let map = lock.get(pset_id);
        map.cloned()
    }

    fn get_all_partition_sets(&self) -> HashMap<Arc<str>, PartitionSetRef<T>> {
        let lock = self.uuid_to_partition_set.read().unwrap();
        lock.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn put_partition_set(&self, pset_id: PartitionId, pset: PartitionSetRef<T>) -> DaftResult<()> {
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

pub type PartitionSetCacheRef<T> = Arc<dyn PartitionSetCache<T>>;

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
impl<T: Partition> PartitionSetCache<T> for PyPartitionSetCache {
    fn get_partition_set(&self, _pset_id: &str) -> Option<PartitionSetRef<T>> {
        todo!("this is all handled in python world right now")
    }

    fn get_all_partition_sets(&self) -> HashMap<Arc<str>, PartitionSetRef<T>> {
        todo!("this is all handled in python world right now")
    }

    fn put_partition_set(
        &self,
        _pset_id: PartitionId,
        _pset: PartitionSetRef<T>,
    ) -> DaftResult<()> {
        todo!("this is all handled in python world right now")
    }

    fn rm(&self, _pset_id: &str) {
        todo!("this is all handled in python world right now")
    }

    fn clear(&self) {
        todo!("this is all handled in python world right now")
    }
}
