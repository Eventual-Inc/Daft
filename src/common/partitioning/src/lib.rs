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
///
/// Acts as a forward declaration for concrete partition implementations. _(Specifically the `MicroPartition` type defined in `daft-micropartition`)_
pub trait Partition: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn size_bytes(&self) -> DaftResult<Option<usize>>;
    fn num_rows(&self) -> DaftResult<usize>;
}

impl<T> Partition for Arc<T>
where
    T: Partition,
{
    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
    fn size_bytes(&self) -> DaftResult<Option<usize>> {
        (**self).size_bytes()
    }

    fn num_rows(&self) -> DaftResult<usize> {
        (**self).num_rows()
    }
}

/// An Arc'd reference to a [`Partition`]
pub type PartitionRef = Arc<dyn Partition>;

/// Key used to identify a partition
pub type PartitionId = usize;

/// ported over from `daft/runners/partitioning.py`
// TODO: port over the rest of the functionality
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub num_rows: usize,
    pub size_bytes: usize,
}

/// A partition set is a collection of partitions.
///
/// It is up to the implementation to decide how to store and manage the partition batches.
/// For example, an in memory partition set could likely be stored as `HashMap<PartitionId, PartitionBatchRef<T>>`.
///
/// It is important to note that the methods do not take `&mut self` but instead take `&self`.
/// So it is up to the implementation to manage any interior mutability.
pub trait PartitionSet<T: Partition>: std::fmt::Debug + Send + Sync {
    /// Merge all micropartitions into a single micropartition
    fn get_merged_partitions(&self) -> DaftResult<PartitionRef>;
    /// Get a preview of the micropartitions
    fn get_preview_partitions(&self, num_rows: usize) -> DaftResult<Vec<T>>;
    /// Number of partitions
    fn num_partitions(&self) -> usize;
    fn len(&self) -> usize;
    /// Check if the partition set is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Size of the partition set in bytes
    fn size_bytes(&self) -> DaftResult<usize>;
    /// Check if a partition exists
    fn has_partition(&self, idx: &PartitionId) -> bool;
    /// Delete a partition
    fn delete_partition(&self, idx: &PartitionId) -> DaftResult<()>;
    /// Set a partition
    fn set_partition(&self, idx: PartitionId, part: &T) -> DaftResult<()>;
    /// Get a partition
    fn get_partition(&self, idx: &PartitionId) -> DaftResult<T>;
    /// Consume the partition set and return a stream of partitions
    fn to_partition_stream(&self) -> BoxStream<'static, DaftResult<T>>;
    fn metadata(&self) -> PartitionMetadata;
}

impl<P, PS> PartitionSet<P> for Arc<PS>
where
    P: Partition + Clone,
    PS: PartitionSet<P> + Clone,
{
    fn get_merged_partitions(&self) -> DaftResult<PartitionRef> {
        PS::get_merged_partitions(self)
    }

    fn get_preview_partitions(&self, num_rows: usize) -> DaftResult<Vec<P>> {
        PS::get_preview_partitions(self, num_rows)
    }

    fn num_partitions(&self) -> usize {
        PS::num_partitions(self)
    }

    fn len(&self) -> usize {
        PS::len(self)
    }

    fn size_bytes(&self) -> DaftResult<usize> {
        PS::size_bytes(self)
    }

    fn has_partition(&self, idx: &PartitionId) -> bool {
        PS::has_partition(self, idx)
    }

    fn delete_partition(&self, idx: &PartitionId) -> DaftResult<()> {
        PS::delete_partition(self, idx)
    }

    fn set_partition(&self, idx: PartitionId, part: &P) -> DaftResult<()> {
        PS::set_partition(self, idx, part)
    }

    fn get_partition(&self, idx: &PartitionId) -> DaftResult<P> {
        PS::get_partition(self, idx)
    }

    fn to_partition_stream(&self) -> BoxStream<'static, DaftResult<P>> {
        PS::to_partition_stream(self)
    }

    fn metadata(&self) -> PartitionMetadata {
        PS::metadata(self)
    }
}

pub type PartitionSetRef<T> = Arc<dyn PartitionSet<T>>;

pub trait PartitionSetCache<P: Partition, PS: PartitionSet<P>>:
    std::fmt::Debug + Send + Sync
{
    fn get_partition_set(&self, key: &str) -> Option<PartitionSetRef<P>>;
    fn get_all_partition_sets(&self) -> Vec<PartitionSetRef<P>>;
    fn put_partition_set(&self, key: &str, partition_set: &PS);
    fn rm_partition_set(&self, key: &str);
    fn clear(&self);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionCacheEntry {
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    #[cfg(feature = "python")]
    /// in python, the partition cache is a weakvalue dictionary, so it will store the entry as long as this reference exists.
    Python(Arc<PyObject>),

    Rust {
        key: String,
        #[serde(skip)]
        /// We don't ever actually reference the value, we're just holding it to ensure the partition set is kept alive.
        ///
        /// It's only wrapped in an `Option` to satisfy serde Deserialize. We skip (de)serializing, but serde still complains if it's not an Option.
        value: Option<Arc<dyn Any + Send + Sync + 'static>>,
    },
}

impl PartitionCacheEntry {
    pub fn new_rust<T: Any + Send + Sync + 'static>(key: String, value: Arc<T>) -> Self {
        Self::Rust {
            key,
            value: Some(value),
        }
    }

    #[cfg(feature = "python")]
    pub fn key(&self) -> String {
        use pyo3::Python;

        match self {
            Self::Python(obj) => Python::with_gil(|py| {
                let key = obj.getattr(py, "key").unwrap();
                key.extract::<String>(py).unwrap()
            }),
            Self::Rust { key, .. } => key.clone(),
        }
    }

    #[cfg(not(feature = "python"))]
    pub fn key(&self) -> String {
        match self {
            PartitionCacheEntry::Rust { key, .. } => key.clone(),
        }
    }
}
