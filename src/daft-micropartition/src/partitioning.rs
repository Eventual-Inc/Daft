use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
pub use common_partitioning::{PartitionBatch, PartitionBatchRef, PartitionMetadata, PartitionSet};
use daft_table::Table;
use futures::stream::BoxStream;

use crate::MicroPartition;
type PartitionId = String;

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

impl PartitionBatch<Arc<MicroPartition>> for InMemoryPartitionBatch {
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
#[derive(Debug, Default)]
pub struct InMemoryPartitionSet {
    pub partitions: HashMap<String, Vec<Arc<MicroPartition>>>,
}

impl InMemoryPartitionSet {
    pub fn new(psets: HashMap<String, Vec<Arc<MicroPartition>>>) -> Self {
        Self { partitions: psets }
    }
}

impl PartitionSet<Arc<MicroPartition>> for InMemoryPartitionSet {
    fn get_merged_partitions(&self) -> DaftResult<Arc<MicroPartition>> {
        let parts = self.values()?;
        let parts = parts.into_iter().flat_map(|mat_res| mat_res.partitions());

        MicroPartition::concat(parts).map(Arc::new)
    }

    fn get_preview_partitions(&self, mut num_rows: usize) -> DaftResult<Vec<Arc<MicroPartition>>> {
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

    fn items(
        &self,
    ) -> DaftResult<
        Vec<(
            PartitionId,
            common_partitioning::PartitionBatchRef<Arc<MicroPartition>>,
        )>,
    > {
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
        let mut partitions = partitions.into_iter().flat_map(|mp| mp.partitions());
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
        part: &dyn common_partitioning::PartitionBatch<Arc<MicroPartition>>,
    ) -> DaftResult<()> {
        let part = part.partitions();

        self.partitions.insert(partition_id, part);
        Ok(())
    }

    fn get_partition(
        &self,
        idx: &PartitionId,
    ) -> DaftResult<common_partitioning::PartitionBatchRef<Arc<MicroPartition>>> {
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
