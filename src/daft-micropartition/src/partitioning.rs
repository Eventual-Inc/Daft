use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_dsl::Expr;
use daft_table::Table;
use futures::stream::BoxStream;

use crate::MicroPartition;
type PartitionId = String;

/// ported over from `daft/runners/partitioning.py`
// TODO: port over the rest of the functionality
#[derive(Debug, Clone)]
pub struct Boundaries {
    pub sort_by: Vec<Expr>,
    pub bounds: Arc<MicroPartition>,
}

/// ported over from `daft/runners/partitioning.py`
// TODO: port over the rest of the functionality
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub num_rows: usize,
    pub size_bytes: usize,
    pub boundaries: Option<Boundaries>,
}

impl PartitionMetadata {
    pub fn from_micro_partition(mp: &MicroPartition) -> Self {
        let num_rows = mp.len();
        let size_bytes = mp.size_bytes().unwrap_or(None).unwrap_or(0);
        Self {
            num_rows,
            size_bytes,
            boundaries: None,
        }
    }
}

/// A collection of related [`MicroPartition`]'s that can be processed as a single unit.
pub trait PartitionBatch: Send + Sync {
    fn micropartitions(&self) -> Vec<Arc<MicroPartition>>;
    fn metadata(&self) -> PartitionMetadata;
    fn into_partition_stream(
        self: Arc<Self>,
    ) -> BoxStream<'static, DaftResult<Arc<MicroPartition>>>;
}

/// an in memory batch of [`MicroPartition`]'s
#[derive(Debug, Clone)]
pub struct InMemoryPartitionBatch {
    pub partition: Vec<Arc<MicroPartition>>,
    pub metadata: Option<PartitionMetadata>,
}

impl InMemoryPartitionBatch {
    pub fn new(partition: Vec<Arc<MicroPartition>>, metadata: Option<PartitionMetadata>) -> Self {
        Self {
            partition,
            metadata,
        }
    }
}

impl TryFrom<Vec<Table>> for InMemoryPartitionBatch {
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

impl PartitionBatch for InMemoryPartitionBatch {
    fn micropartitions(&self) -> Vec<Arc<MicroPartition>> {
        self.partition.clone()
    }

    fn metadata(&self) -> PartitionMetadata {
        if let Some(metadata) = &self.metadata {
            metadata.clone()
        } else if self.partition.is_empty() {
            PartitionMetadata {
                num_rows: 0,
                size_bytes: 0,
                boundaries: None,
            }
        } else {
            PartitionMetadata::from_micro_partition(&self.partition[0])
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

/// an arc'd reference to a [`PartitionBatch`]
pub type PartitionBatchRef = Arc<dyn PartitionBatch>;

/// a collection of [`MicroPartition`]
///
/// Since we can have different partition sets such as an in memory, or a distributed partition set, we need to abstract over the partition set.
/// This trait defines the common operations that can be performed on a partition set.
pub trait PartitionSet {
    /// Merge all micropartitions into a single micropartition
    fn get_merged_micropartitions(&self) -> DaftResult<MicroPartition>;
    /// Get a preview of the micropartitions
    fn get_preview_micropartitions(&self, num_rows: usize) -> DaftResult<Vec<Arc<MicroPartition>>>;
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
    fn set_partition(&mut self, idx: PartitionId, part: PartitionBatchRef) -> DaftResult<()>;
    /// Get a partition
    fn get_partition(&self, idx: &PartitionId) -> DaftResult<PartitionBatchRef>;
}

/// An in memory partition set
#[derive(Debug, Default)]
pub struct InMemoryPartitionSet {
    pub partitions: HashMap<String, Vec<Arc<MicroPartition>>>,
}

impl InMemoryPartitionSet {
    pub fn new(psets: HashMap<String, Vec<Arc<MicroPartition>>>) -> Self {
        Self { partitions: psets }
    }
}

impl PartitionSet for InMemoryPartitionSet {
    fn get_merged_micropartitions(&self) -> DaftResult<MicroPartition> {
        let parts = self.values()?;
        let parts = parts
            .into_iter()
            .flat_map(|mat_res| mat_res.micropartitions());

        MicroPartition::concat(parts)
    }

    fn get_preview_micropartitions(
        &self,
        mut num_rows: usize,
    ) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let mut preview_parts = vec![];

        for part in self.partitions.values().flatten() {
            let part_len = part.len();
            if part_len >= num_rows {
                preview_parts.push(Arc::new(part.slice(0, num_rows)?));
                break;
            } else {
                num_rows -= part_len;
                preview_parts.push(part.clone());
            }
        }
        Ok(preview_parts)
    }

    fn items(&self) -> DaftResult<Vec<(PartitionId, PartitionBatchRef)>> {
        self.partitions
            .iter()
            .map(|(k, v)| {
                let partition = InMemoryPartitionBatch {
                    partition: v.clone(),
                    metadata: None,
                };

                Ok((k.clone(), Arc::new(partition) as _))
            })
            .collect()
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
        let partitions = self.values()?;
        let mut partitions = partitions.into_iter().flat_map(|mp| mp.micropartitions());
        partitions.try_fold(0, |acc, mp| Ok(acc + mp.size_bytes()?.unwrap_or(0)))
    }

    fn has_partition(&self, partition_id: &PartitionId) -> bool {
        self.partitions.contains_key(partition_id)
    }

    fn delete_partition(&mut self, partition_id: &PartitionId) -> DaftResult<()> {
        self.partitions.remove(partition_id);
        Ok(())
    }

    fn set_partition(
        &mut self,
        partition_id: PartitionId,
        part: PartitionBatchRef,
    ) -> DaftResult<()> {
        let part = part.micropartitions();

        self.partitions.insert(partition_id, part);
        Ok(())
    }

    fn get_partition(&self, idx: &PartitionId) -> DaftResult<PartitionBatchRef> {
        let part = self
            .partitions
            .get(idx)
            .ok_or(DaftError::ValueError("Partition not found".to_string()))?;

        Ok(Arc::new(InMemoryPartitionBatch {
            partition: part.clone(),
            metadata: None,
        }))
    }
}
