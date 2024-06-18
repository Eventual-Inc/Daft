use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use super::{partition_ref::PartitionMetadata, PartitionRef};

/// A virtual partition interface that can provide:
/// 1. metadata for the partition,
/// 2. the concrete materialized partition, which can be given as the input to a corresponding PartitionTaskOp.
///
/// This encompasses both PartitionRef implementations and ScanTasks.
pub trait VirtualPartition: Clone {
    type TaskOpInput;

    /// Metadata for partition.
    fn metadata(&self) -> PartitionMetadata;

    /// Concrete materialized partition, which can be given as the input to a corresponding PartitionTaskOp.
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

/// A set of partitions represented as either PartitionRefs or ScanTasks.
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
