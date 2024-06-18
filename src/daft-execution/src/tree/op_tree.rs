use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::ops::PartitionTaskOp;

#[derive(Debug, Clone)]
pub struct LeafScanNode {
    pub task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
}

impl LeafScanNode {
    pub fn new(task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>) -> Self {
        Self { task_op }
    }
}

impl<T> From<T> for LeafScanNode
where
    T: PartitionTaskOp<Input = ScanTask> + 'static,
{
    fn from(value: T) -> Self {
        Self::new(Arc::new(value))
    }
}

#[derive(Debug, Clone)]
pub struct LeafMemoryNode {
    pub task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
}

impl LeafMemoryNode {
    pub fn new(task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>) -> Self {
        Self { task_op }
    }
}

impl From<Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>> for LeafMemoryNode {
    fn from(value: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone)]
pub struct InnerNode {
    pub inputs: Vec<OpNode>,
    pub task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
}

impl InnerNode {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
        inputs: Vec<OpNode>,
    ) -> Self {
        Self { inputs, task_op }
    }
}

impl<T> From<(T, Vec<OpNode>)> for InnerNode
where
    T: PartitionTaskOp<Input = MicroPartition> + 'static,
{
    fn from(value: (T, Vec<OpNode>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug, Clone)]
pub enum OpNode {
    LeafScan(LeafScanNode),
    LeafMemory(LeafMemoryNode),
    Inner(InnerNode),
}

impl OpNode {
    pub fn num_outputs(&self) -> usize {
        match self {
            Self::LeafScan(LeafScanNode { task_op }) => task_op.num_outputs(),
            Self::LeafMemory(LeafMemoryNode { task_op }) => {
                task_op.as_ref().map(|op| op.num_outputs()).unwrap_or(1)
            }
            Self::Inner(InnerNode { task_op, .. }) => task_op.num_outputs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_micropartition::MicroPartition;

    use daft_scan::ScanTask;
    use rstest::rstest;

    use crate::test::MockOutputOp;

    use super::{InnerNode, LeafMemoryNode, LeafScanNode, OpNode};

    /// Tests that number of outputs for an OpNode::LeafScan is propagated from the underlying task op.
    #[rstest]
    fn num_outputs_leaf_scan(#[values(1, 2, 10)] num_outputs: usize) -> DaftResult<()> {
        let op = MockOutputOp::<ScanTask>::new("op", num_outputs);
        let node = OpNode::LeafScan(LeafScanNode::new(Arc::new(op)));
        assert_eq!(node.num_outputs(), num_outputs);
        Ok(())
    }

    /// Tests that number of outputs for an OpNode::LeafMemory is propagated from the underlying task op.
    #[rstest]
    fn num_outputs_leaf_memory(#[values(1, 2, 10)] num_outputs: usize) -> DaftResult<()> {
        let op = MockOutputOp::<MicroPartition>::new("op", num_outputs);
        let node = OpNode::LeafMemory(LeafMemoryNode::new(Some(Arc::new(op))));
        assert_eq!(node.num_outputs(), num_outputs);
        Ok(())
    }

    /// Tests that number of outputs for an OpNode::Inner is propagated from the underlying task op.
    #[rstest]
    fn num_outputs_inner(#[values(1, 2, 10)] num_outputs: usize) -> DaftResult<()> {
        let op = MockOutputOp::<MicroPartition>::new("op", num_outputs);
        let node = OpNode::Inner(InnerNode::new(Arc::new(op), vec![]));
        assert_eq!(node.num_outputs(), num_outputs);
        Ok(())
    }
}
