use std::{any::Any, sync::Arc};

use common_error::DaftResult;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    common_py_serde::{deserialize_py_object, serialize_py_object},
    pyo3::PyObject,
};

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
    fn into_partition_stream(self: Arc<Self>) -> BoxStream<'static, DaftResult<Arc<T>>>;
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
///
/// It is important to note that the methods do not take `&mut self` but instead take `&self`.
/// So it is up to the implementation to manage any interior mutability.
pub trait PartitionSet<T: Partition>: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
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
    fn delete_partition(&self, idx: &PartitionId) -> DaftResult<()>;
    /// Set a partition
    fn set_partition(&self, idx: PartitionId, part: &dyn PartitionBatch<T>) -> DaftResult<()>;
    /// Get a partition
    fn get_partition(&self, idx: &PartitionId) -> DaftResult<PartitionBatchRef<T>>;

    /// Consume the partition set and return a stream of partitions
    fn into_partition_stream(self: Arc<Self>) -> BoxStream<'static, DaftResult<Arc<T>>>;
}

pub type PartitionSetRef<T> = Arc<dyn PartitionSet<T>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionCacheEntry {
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[cfg(feature = "python")]
    Python(PyObject),
    Rust(String),
}

impl PartitionCacheEntry {
    pub fn new_rust(key: String) -> Self {
        Self::Rust(key)
    }

    #[cfg(feature = "python")]
    pub fn new_py(value: PyObject) -> Self {
        Self::Python(value)
    }
}
