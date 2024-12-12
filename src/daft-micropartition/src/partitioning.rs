use std::{any::Any, sync::Arc};

use common_error::{DaftError, DaftResult};
pub use common_partitioning::*;
use daft_table::Table;
use dashmap::DashMap;
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
    pub partitions: DashMap<PartitionId, Vec<Arc<MicroPartition>>>,
}

impl MicroPartitionSet {
    pub fn new<T: IntoIterator<Item = (PartitionId, Vec<Arc<MicroPartition>>)>>(psets: T) -> Self {
        Self {
            partitions: psets.into_iter().collect(),
        }
    }
    pub fn empty() -> Self {
        Self::default()
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
        let parts = self.partitions.iter().flat_map(|v| v.value().clone());
        MicroPartition::concat(parts).map(|mp| Arc::new(mp) as _)
    }

    fn get_preview_partitions(
        &self,
        mut num_rows: usize,
    ) -> DaftResult<PartitionBatchRef<MicroPartition>> {
        let mut preview_parts = vec![];

        for part in self.partitions.iter().flat_map(|v| v.value().clone()) {
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
        let mut parts = self.partitions.iter().flat_map(|v| v.value().clone());

        parts.try_fold(0, |acc, mp| Ok(acc + mp.size_bytes()?.unwrap_or(0)))
    }

    fn has_partition(&self, partition_id: &PartitionId) -> bool {
        self.partitions.contains_key(partition_id.as_ref())
    }

    fn delete_partition(&self, partition_id: &PartitionId) -> DaftResult<()> {
        self.partitions.remove(partition_id.as_ref());
        Ok(())
    }

    fn set_partition(
        &self,
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
        let partitions = self.partitions.clone();
        Box::pin(futures::stream::iter(
            partitions
                .into_iter()
                .flat_map(|(_, v)| v.into_iter())
                .map(Ok),
        ))
    }
}
