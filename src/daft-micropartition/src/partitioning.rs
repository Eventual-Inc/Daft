use std::{
    any::Any,
    sync::{Arc, Weak},
};

use common_error::{DaftError, DaftResult};
pub use common_partitioning::*;
use daft_recordbatch::RecordBatch;
use dashmap::DashMap;
use futures::stream::BoxStream;

use crate::{micropartition::MicroPartitionRef, MicroPartition};

impl Partition for MicroPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn size_bytes(&self) -> DaftResult<Option<usize>> {
        self.size_bytes()
    }
    fn num_rows(&self) -> DaftResult<usize> {
        Ok(self.len())
    }
}

// An in memory partition set
#[derive(Debug, Default, Clone)]
pub struct MicroPartitionSet {
    pub partitions: DashMap<PartitionId, MicroPartitionRef>,
}

impl From<Vec<MicroPartitionRef>> for MicroPartitionSet {
    fn from(value: Vec<MicroPartitionRef>) -> Self {
        let partitions = value
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as PartitionId, v))
            .collect();
        Self { partitions }
    }
}

impl MicroPartitionSet {
    pub fn new<T: IntoIterator<Item = (PartitionId, MicroPartitionRef)>>(psets: T) -> Self {
        Self {
            partitions: psets.into_iter().collect(),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_record_batches(
        id: PartitionId,
        record_batches: Vec<RecordBatch>,
    ) -> DaftResult<Self> {
        if record_batches.is_empty() {
            return Ok(Self::empty());
        }
        let schema = &record_batches[0].schema;
        let mp = MicroPartition::new_loaded(schema.clone(), Arc::new(record_batches), None);
        Ok(Self::new(vec![(id, Arc::new(mp))]))
    }
}

impl PartitionSet<MicroPartitionRef> for MicroPartitionSet {
    fn get_merged_partitions(&self) -> DaftResult<PartitionRef> {
        let parts = self.partitions.iter().map(|v| v.value().clone());
        MicroPartition::concat(parts).map(|mp| Arc::new(mp) as _)
    }

    fn get_preview_partitions(&self, mut num_rows: usize) -> DaftResult<Vec<MicroPartitionRef>> {
        let mut preview_parts = vec![];

        for part in self.partitions.iter().map(|v| v.value().clone()) {
            let part_len = part.len();
            if part_len >= num_rows {
                let mp = part.slice(0, num_rows)?;
                let part = Arc::new(mp);

                preview_parts.push(part);
                break;
            } else {
                num_rows -= part_len;
                preview_parts.push(part.clone());
            }
        }
        Ok(preview_parts)
    }

    fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    fn len(&self) -> usize {
        self.partitions.len()
    }

    fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    fn size_bytes(&self) -> DaftResult<usize> {
        let mut parts = self.partitions.iter().map(|v| v.value().clone());

        parts.try_fold(0, |acc, mp| Ok(acc + mp.size_bytes()?.unwrap_or(0)))
    }

    fn has_partition(&self, partition_id: &PartitionId) -> bool {
        self.partitions.contains_key(partition_id)
    }

    fn delete_partition(&self, partition_id: &PartitionId) -> DaftResult<()> {
        self.partitions.remove(partition_id);
        Ok(())
    }

    fn set_partition(&self, partition_id: PartitionId, part: &MicroPartitionRef) -> DaftResult<()> {
        self.partitions.insert(partition_id, part.clone());
        Ok(())
    }

    fn get_partition(&self, idx: &PartitionId) -> DaftResult<MicroPartitionRef> {
        let part = self
            .partitions
            .get(idx)
            .ok_or(DaftError::ValueError("Partition not found".to_string()))?;

        Ok(part.clone())
    }

    fn to_partition_stream(&self) -> BoxStream<'static, DaftResult<MicroPartitionRef>> {
        let partitions = self.partitions.clone().into_iter().map(|(_, v)| v).map(Ok);

        Box::pin(futures::stream::iter(partitions))
    }

    fn metadata(&self) -> PartitionMetadata {
        let size_bytes = self.size_bytes().unwrap_or(0);
        let num_rows = self.partitions.iter().map(|v| v.value().len()).sum();
        PartitionMetadata {
            num_rows,
            size_bytes,
        }
    }
}

/// An in-memory cache for partition sets
///
/// Note: this holds weak references to the partition sets. It's structurally similar to a WeakValueHashMap
///
/// This means that if the partition set is dropped, it will be removed from the cache.
/// So the partition set must outlive the lifetime of the value in the cache.
///
/// if the partition set is dropped before the cache, it will be removed
/// ex:
/// ```rust,no_run
///
///  let cache = InMemoryPartitionSetCache::empty();
///  let outer =Arc::new(MicroPartitionSet::empty());
///  cache.put_partition_set("outer", &outer);
/// {
///   let inner = Arc::new(MicroPartitionSet::empty());
///   cache.put_partition_set("inner", &inner);
///   cache.get_partition_set("inner"); // Some(inner)
///   // inner is dropped here
/// }
///
/// cache.get_partition_set("inner"); // None
/// cache.get_partition_set("outer"); // Some(outer)
/// drop(outer);
/// cache.get_partition_set("outer"); // None
/// ```
#[derive(Debug, Default, Clone)]
pub struct InMemoryPartitionSetCache {
    pub partition_sets: DashMap<String, Weak<MicroPartitionSet>>,
}

impl InMemoryPartitionSetCache {
    pub fn new<'a, T: IntoIterator<Item = (&'a String, &'a Arc<MicroPartitionSet>)>>(
        psets: T,
    ) -> Self {
        Self {
            partition_sets: psets
                .into_iter()
                .map(|(k, v)| (k.clone(), Arc::downgrade(v)))
                .collect(),
        }
    }
    pub fn empty() -> Self {
        Self::default()
    }
}

impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> for InMemoryPartitionSetCache {
    fn get_partition_set(&self, key: &str) -> Option<PartitionSetRef<MicroPartitionRef>> {
        let weak_pset = self.partition_sets.get(key).map(|v| v.value().clone())?;
        // if the partition set has been dropped, remove it from the cache
        let Some(pset) = weak_pset.upgrade() else {
            tracing::trace!("Removing dropped partition set from cache: {}", key);
            self.partition_sets.remove(key);
            return None;
        };

        Some(pset as _)
    }

    fn get_all_partition_sets(&self) -> Vec<PartitionSetRef<MicroPartitionRef>> {
        let psets = self.partition_sets.iter().filter_map(|v| {
            let pset = v.value().upgrade()?;
            Some(pset as _)
        });

        psets.collect()
    }

    fn put_partition_set(&self, key: &str, partition_set: &Arc<MicroPartitionSet>) {
        self.partition_sets
            .insert(key.to_string(), Arc::downgrade(partition_set));
    }

    fn rm_partition_set(&self, key: &str) {
        self.partition_sets.remove(key);
    }

    fn clear(&self) {
        self.partition_sets.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_drops_pset() {
        let cache = InMemoryPartitionSetCache::empty();

        {
            let pset = Arc::new(MicroPartitionSet::empty());
            cache.put_partition_set("key", &pset);
            assert!(cache.get_partition_set("key").is_some());
        }

        assert!(cache.get_partition_set("key").is_none());
    }
}
