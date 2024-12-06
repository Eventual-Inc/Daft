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

pub trait MaterializedResult<T> {
    fn as_any(&self) -> &dyn Any;
    fn partition(&self) -> T;
    /// returns the result as a single micropartition.
    fn micropartition(&self) -> Arc<MicroPartition>;
    /// If the result has multiple micropartitions, returns all of them
    fn micropartitions(&self) -> Vec<Arc<MicroPartition>> {
        vec![self.micropartition()]
    }
    fn metadata(&self) -> PartitionMetadata;
}

#[derive(Debug, Clone)]
pub struct LocalMaterializedResult {
    pub partition: Vec<Arc<MicroPartition>>,
    pub metadata: Option<PartitionMetadata>,
}

impl MaterializedResult<Vec<Arc<MicroPartition>>> for LocalMaterializedResult {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn partition(&self) -> Vec<Arc<MicroPartition>> {
        self.partition.clone()
    }

    fn micropartition(&self) -> Arc<MicroPartition> {
        Arc::new(MicroPartition::concat(self.partition.clone()).unwrap())
    }
    fn micropartitions(&self) -> Vec<Arc<MicroPartition>> {
        self.partition.clone()
    }

    fn metadata(&self) -> PartitionMetadata {
        if let Some(metadata) = &self.metadata {
            metadata.clone()
        } else {
            PartitionMetadata::from_micro_partition(&self.partition[0])
        }
    }
}

type MaterializedPartitionSet = Arc<dyn MaterializedResult<Vec<Arc<MicroPartition>>>>;

pub trait PartitionSet {
    fn get_merged_micropartitions(&self) -> DaftResult<MicroPartition>;
    fn get_preview_micropartitions(&self, num_rows: usize) -> DaftResult<Vec<Arc<MicroPartition>>>;
    fn items(&self) -> DaftResult<Vec<(PartId, MaterializedPartitionSet)>>;
    fn values(&self) -> DaftResult<Vec<MaterializedPartitionSet>> {
        let items = self.items()?;
        Ok(items.into_iter().map(|(_, mp)| mp).collect())
    }

    fn num_partitions(&self) -> usize;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn size_bytes(&self) -> DaftResult<usize>;
    fn has_partition(&self, idx: &PartId) -> bool;
    fn delete_partition(&mut self, idx: &PartId) -> DaftResult<()>;
    fn set_partition(&mut self, idx: PartId, part: MaterializedPartitionSet) -> DaftResult<()>;
    fn get_partition(&self, idx: &PartId) -> DaftResult<MaterializedPartitionSet>;
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
        let parts = parts.into_iter().map(|mat_res| mat_res.micropartition());

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

    fn items(&self) -> DaftResult<Vec<(PartId, MaterializedPartitionSet)>> {
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
        let mut partitions = partitions.into_iter().map(|mp| mp.micropartition());
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
        part: MaterializedPartitionSet,
    ) -> DaftResult<()> {
        let part = part.micropartitions();

        self.partitions.insert(partition_id, part);
        Ok(())
    }

    fn get_partition(&self, idx: &PartId) -> DaftResult<MaterializedPartitionSet> {
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
