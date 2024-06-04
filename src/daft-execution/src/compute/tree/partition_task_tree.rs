use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::compute::ops::PartitionTaskOp;

#[derive(Debug, Clone)]
pub struct PartitionTaskLeafScanNode {
    pub task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
}

impl PartitionTaskLeafScanNode {
    pub fn new(task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>) -> Self {
        Self { task_op }
    }
}

impl<T> From<T> for PartitionTaskLeafScanNode
where
    T: PartitionTaskOp<Input = ScanTask> + 'static,
{
    fn from(value: T) -> Self {
        Self::new(Arc::new(value))
    }
}

#[derive(Debug, Clone)]
pub struct PartitionTaskLeafMemoryNode {
    pub task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
}

impl PartitionTaskLeafMemoryNode {
    pub fn new(task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>) -> Self {
        Self { task_op }
    }
}

impl From<Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>>
    for PartitionTaskLeafMemoryNode
{
    fn from(value: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionTaskInnerNode {
    pub inputs: Vec<PartitionTaskNode>,
    pub task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
}

impl PartitionTaskInnerNode {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
        inputs: Vec<PartitionTaskNode>,
    ) -> Self {
        Self { inputs, task_op }
    }
}

impl<T> From<(T, Vec<PartitionTaskNode>)> for PartitionTaskInnerNode
where
    T: PartitionTaskOp<Input = MicroPartition> + 'static,
{
    fn from(value: (T, Vec<PartitionTaskNode>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug, Clone)]
pub enum PartitionTaskNode {
    LeafScan(PartitionTaskLeafScanNode),
    LeafMemory(PartitionTaskLeafMemoryNode),
    Inner(PartitionTaskInnerNode),
}

impl PartitionTaskNode {
    pub fn num_outputs(&self) -> usize {
        match self {
            Self::LeafScan(PartitionTaskLeafScanNode { task_op }) => task_op.num_outputs(),
            Self::LeafMemory(PartitionTaskLeafMemoryNode { task_op }) => {
                task_op.as_ref().map(|op| op.num_outputs()).unwrap_or(1)
            }
            Self::Inner(PartitionTaskInnerNode { task_op, .. }) => task_op.num_outputs(),
        }
    }
}
