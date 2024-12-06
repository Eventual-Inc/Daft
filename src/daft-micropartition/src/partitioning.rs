use std::{any::Any, collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_dsl::Expr;

use crate::MicroPartition;
type PartId = String;

#[derive(Debug, Clone)]
pub struct Boundaries {
    pub sort_by: Vec<Expr>,
    pub bounds: Arc<MicroPartition>,
}

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

pub trait MaterializedResult {
    fn as_any(&self) -> &dyn Any;
    fn micropartitions(&self) -> Vec<Arc<MicroPartition>>;
    fn metadata(&self) -> PartitionMetadata;
}

#[derive(Debug, Clone)]
pub struct LocalMaterializedResult {
    pub partition: Vec<Arc<MicroPartition>>,
    pub metadata: Option<PartitionMetadata>,
}

impl MaterializedResult for LocalMaterializedResult {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
}

type MaterializedResultRef = Arc<dyn MaterializedResult>;

pub trait PartitionSet {
    /// Merge all micropartitions into a single micropartition
    fn get_merged_micropartitions(&self) -> DaftResult<MicroPartition>;
    /// Get a preview of the micropartitions
    fn get_preview_micropartitions(&self, num_rows: usize) -> DaftResult<Vec<Arc<MicroPartition>>>;
    fn items(&self) -> DaftResult<Vec<(PartId, MaterializedResultRef)>>;
    fn values(&self) -> DaftResult<Vec<MaterializedResultRef>> {
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
    fn has_partition(&self, idx: &PartId) -> bool;
    /// Delete a partition
    fn delete_partition(&mut self, idx: &PartId) -> DaftResult<()>;
    /// Set a partition
    fn set_partition(&mut self, idx: PartId, part: MaterializedResultRef) -> DaftResult<()>;
    /// Get a partition
    fn get_partition(&self, idx: &PartId) -> DaftResult<MaterializedResultRef>;
}

#[derive(Debug, Default)]
pub struct LocalPartitionSet {
    pub partitions: HashMap<String, Vec<Arc<MicroPartition>>>,
}

impl LocalPartitionSet {
    pub fn new(psets: HashMap<String, Vec<Arc<MicroPartition>>>) -> Self {
        Self { partitions: psets }
    }
}

impl PartitionSet for LocalPartitionSet {
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

    fn items(&self) -> DaftResult<Vec<(PartId, MaterializedResultRef)>> {
        self.partitions
            .iter()
            .map(|(k, v)| {
                let partition = LocalMaterializedResult {
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

    fn has_partition(&self, partition_id: &PartId) -> bool {
        self.partitions.contains_key(partition_id)
    }

    fn delete_partition(&mut self, partition_id: &PartId) -> DaftResult<()> {
        self.partitions.remove(partition_id);
        Ok(())
    }

    fn set_partition(
        &mut self,
        partition_id: PartId,
        part: MaterializedResultRef,
    ) -> DaftResult<()> {
        let part = part.micropartitions();

        self.partitions.insert(partition_id, part);
        Ok(())
    }

    fn get_partition(&self, idx: &PartId) -> DaftResult<MaterializedResultRef> {
        let part = self
            .partitions
            .get(idx)
            .ok_or(DaftError::ValueError("Partition not found".to_string()))?;

        Ok(Arc::new(LocalMaterializedResult {
            partition: part.clone(),
            metadata: None,
        }))
    }
}
