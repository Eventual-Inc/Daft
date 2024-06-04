use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use super::{partition_ref::PartitionMetadata, PartitionRef};

pub trait VirtualPartition: Clone {
    type TaskOpInput;

    fn metadata(&self) -> PartitionMetadata;
    fn partition(&self) -> Arc<Self::TaskOpInput>;
}

impl<T: PartitionRef> VirtualPartition for T {
    type TaskOpInput = MicroPartition;

    fn metadata(&self) -> PartitionMetadata {
        self.metadata()
    }

    fn partition(&self) -> Arc<Self::TaskOpInput> {
        self.partition()
    }
}

impl VirtualPartition for Arc<ScanTask> {
    type TaskOpInput = ScanTask;

    fn metadata(&self) -> PartitionMetadata {
        PartitionMetadata::new(self.num_rows(), self.size_bytes())
    }

    fn partition(&self) -> Arc<Self::TaskOpInput> {
        self.clone()
    }
}

#[derive(Debug, Clone)]
pub enum VirtualPartitionSet<T: PartitionRef> {
    PartitionRef(Vec<T>),
    ScanTask(Vec<Arc<ScanTask>>),
}

impl<T: PartitionRef> VirtualPartitionSet<T> {
    #[allow(unused)]
    pub fn num_partitions(&self) -> usize {
        match self {
            Self::PartitionRef(parts) => parts.len(),
            Self::ScanTask(parts) => parts.len(),
        }
    }
}
