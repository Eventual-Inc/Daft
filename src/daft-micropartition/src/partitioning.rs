use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_error::{DaftError, DaftResult};
pub use common_partitioning::*;
use daft_table::Table;
use futures::stream::BoxStream;

use crate::MicroPartition;

impl Partition for MicroPartition {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn size_bytes(&self) -> DaftResult<Option<usize>> {
        self.size_bytes()
    }
}

/// an in memory batch of [`MicroPartition`]'s
#[derive(Debug, Clone)]
pub struct MicroPartitionBatch {
    pub partition: Vec<Arc<MicroPartition>>,
    pub metadata: Option<PartitionMetadata>,
}

impl MicroPartitionBatch {
    pub fn new(partition: Vec<Arc<MicroPartition>>, metadata: Option<PartitionMetadata>) -> Self {
        Self {
            partition,
            metadata,
        }
    }
}

impl TryFrom<Vec<Table>> for MicroPartitionBatch {
    type Error = DaftError;

    fn try_from(tables: Vec<Table>) -> Result<Self, Self::Error> {
        if tables.is_empty() {
            return Ok(Self {
                partition: vec![],
                metadata: None,
            });
        }

        let schema = &tables[0].schema;
        let mp = MicroPartition::new_loaded(schema.clone(), Arc::new(tables), None);
        Ok(Self {
            partition: vec![Arc::new(mp)],
            metadata: None,
        })
    }
}

impl PartitionBatch<MicroPartition> for MicroPartitionBatch {
    fn partitions(&self) -> Vec<Arc<MicroPartition>> {
        self.partition.clone()
    }

    fn metadata(&self) -> PartitionMetadata {
        if let Some(metadata) = &self.metadata {
            metadata.clone()
        } else if self.partition.is_empty() {
            PartitionMetadata {
                num_rows: 0,
                size_bytes: 0,
            }
        } else {
            let mp = &self.partition[0];
            let num_rows = mp.len();
            let size_bytes = mp.size_bytes().unwrap_or(None).unwrap_or(0);
            PartitionMetadata {
                num_rows,
                size_bytes,
            }
        }
    }

    fn into_partition_stream(
        self: Arc<Self>,
    ) -> BoxStream<'static, DaftResult<Arc<MicroPartition>>> {
        Box::pin(futures::stream::iter(
            self.partition.clone().into_iter().map(Ok),
        ))
    }
}

// An in memory partition set
#[derive(Debug, Default, Clone)]
pub struct MicroPartitionSet {
    pub partitions: HashMap<PartitionId, Vec<Arc<MicroPartition>>>,
}

impl MicroPartitionSet {
    pub fn new(psets: HashMap<PartitionId, Vec<Arc<MicroPartition>>>) -> Self {
        Self { partitions: psets }
    }
}

impl PartitionSet<MicroPartition> for MicroPartitionSet {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
    fn get_merged_partitions(&self) -> DaftResult<PartitionRef> {
        let parts = self.partitions.values();
        let parts = parts.into_iter().flatten();
        MicroPartition::concat(parts).map(|mp| Arc::new(mp) as _)
    }

    fn get_preview_partitions(
        &self,
        mut num_rows: usize,
    ) -> DaftResult<PartitionBatchRef<MicroPartition>> {
        let mut preview_parts = vec![];

        for part in self.partitions.values().flatten() {
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
        let preview_parts = MicroPartitionBatch {
            partition: preview_parts,
            metadata: None,
        };
        Ok(Arc::new(preview_parts))
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
        let partitions = self.partitions.values();

        let mut partitions = partitions.into_iter().flatten();

        partitions.try_fold(0, |acc, mp| Ok(acc + mp.size_bytes()?.unwrap_or(0)))
    }

    fn has_partition(&self, partition_id: &PartitionId) -> bool {
        self.partitions.contains_key(partition_id.as_ref())
    }

    fn delete_partition(&mut self, partition_id: &PartitionId) -> DaftResult<()> {
        self.partitions.remove(partition_id.as_ref());
        Ok(())
    }

    fn set_partition(
        &mut self,
        partition_id: PartitionId,
        part: &dyn PartitionBatch<MicroPartition>,
    ) -> DaftResult<()> {
        let part = part.partitions();

        self.partitions.insert(partition_id, part);
        Ok(())
    }

    fn get_partition(&self, idx: &PartitionId) -> DaftResult<PartitionBatchRef<MicroPartition>> {
        let part = self
            .partitions
            .get(idx)
            .ok_or(DaftError::ValueError("Partition not found".to_string()))?;

        Ok(Arc::new(MicroPartitionBatch {
            partition: part.clone(),
            metadata: None,
        }))
    }

    fn into_partition_stream(
        self: Arc<Self>,
    ) -> BoxStream<'static, DaftResult<Arc<MicroPartition>>> {
        Box::pin(futures::stream::iter(
            self.partitions
                .clone()
                .into_values()
                .flat_map(|v| v.into_iter().map(Ok)),
        ))
    }
}

/// An in memory cache for partition sets
/// This is a simple in memory cache that stores a single partition set
///
/// TODO: this should drop partitions when the reference count for the partition id drops to 0
///
/// TODO: **This will continue to hold onto partitions indefinitely**
///
#[derive(Debug, Clone)]
pub struct InMemoryPartitionSetCache {
    pub inner: Arc<RwLock<MicroPartitionSet>>,
}

impl Default for InMemoryPartitionSetCache {
    fn default() -> Self {
        Self::empty()
    }
}

impl InMemoryPartitionSetCache {
    pub fn new(pset: MicroPartitionSet) -> Self {
        Self {
            inner: Arc::new(RwLock::new(pset)),
        }
    }
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MicroPartitionSet::default())),
        }
    }
}

impl PartitionSetCache<MicroPartition> for InMemoryPartitionSetCache {
    fn get_partition_set(
        &self,
        _pset_id: &str,
    ) -> DaftResult<Option<PartitionSetRef<MicroPartition>>> {
        let lock = self
            .inner
            .read()
            .map_err(|_| DaftError::InternalError("Failed to acquire read lock".to_string()))?;

        Ok(Some(Arc::new(lock.clone())))
    }

    fn get_all_partition_sets(
        &self,
    ) -> DaftResult<HashMap<Arc<str>, PartitionSetRef<MicroPartition>>> {
        let lock = self
            .inner
            .read()
            .map_err(|_| DaftError::InternalError("Failed to acquire read lock".to_string()))?;
        let mut hmap = HashMap::new();
        hmap.insert(Arc::from("default"), Arc::new(lock.clone()) as _);
        Ok(hmap)
    }

    fn put_partition_set(
        &self,
        _pset: PartitionSetRef<MicroPartition>,
    ) -> DaftResult<PartitionCacheEntry> {
        Err(DaftError::InternalError("Not implemented".to_string()))
    }

    fn rm(&self, _pset_id: &str) -> DaftResult<()> {
        Err(DaftError::InternalError("Not implemented".to_string()))
    }

    fn clear(&self) -> DaftResult<()> {
        Err(DaftError::InternalError("Not implemented".to_string()))
    }
}
