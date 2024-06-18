use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use crate::ops::{FusedOpBuilder, PartitionTaskOp};

use super::op_tree::{InnerNode, LeafMemoryNode, LeafScanNode, OpNode};

#[derive(Debug, Clone)]
pub enum PartitionTaskNodeBuilder {
    LeafScan(FusedOpBuilder<ScanTask>),
    LeafMemory(Option<FusedOpBuilder<MicroPartition>>),
    Inner(Vec<OpNode>, FusedOpBuilder<MicroPartition>),
}

impl PartitionTaskNodeBuilder {
    pub fn add_op(&mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) {
        match self {
            Self::LeafScan(builder) => builder.add_op(op),
            Self::LeafMemory(Some(builder)) | Self::Inner(_, builder) => builder.add_op(op),
            Self::LeafMemory(ref mut builder) => {
                let _ = builder.insert(FusedOpBuilder::new(op));
            }
        }
    }

    pub fn can_add_op(&self, op: &dyn PartitionTaskOp<Input = MicroPartition>) -> bool {
        match self {
            Self::LeafScan(builder) => builder.can_add_op(op),
            Self::LeafMemory(Some(builder)) | Self::Inner(_, builder) => builder.can_add_op(op),
            Self::LeafMemory(None) => true,
        }
    }

    pub fn build(self) -> OpNode {
        match self {
            Self::LeafScan(builder) => OpNode::LeafScan(LeafScanNode::new(builder.build())),
            Self::LeafMemory(builder) => {
                OpNode::LeafMemory(LeafMemoryNode::new(builder.map(|b| b.build())))
            }
            Self::Inner(inputs, builder) => OpNode::Inner(InnerNode::new(builder.build(), inputs)),
        }
    }

    pub fn fuse_or_link(mut self, op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>) -> Self {
        if self.can_add_op(op.as_ref()) {
            self.add_op(op.clone());
            self
        } else {
            let op_builder = FusedOpBuilder::new(op);
            let child_node = self.build();
            PartitionTaskNodeBuilder::Inner(vec![child_node], op_builder)
        }
    }
}
