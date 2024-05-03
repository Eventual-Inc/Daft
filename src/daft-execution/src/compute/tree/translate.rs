use std::rc::Rc;

use crate::compute::{
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    tree::partition_task_state_tree::PartitionTaskLeafMemoryState,
};

use super::{
    partition_task_state_tree::{
        PartitionTaskInnerState, PartitionTaskLeafScanState, PartitionTaskState,
    },
    partition_task_tree::{
        PartitionTaskInnerNode, PartitionTaskLeafMemoryNode, PartitionTaskLeafScanNode,
        PartitionTaskNode,
    },
};

pub fn task_tree_to_state_tree<T: PartitionRef>(
    root: PartitionTaskNode,
    leaf_inputs: &mut Vec<VirtualPartitionSet<T>>,
) -> Rc<PartitionTaskState<T>> {
    match root {
        PartitionTaskNode::LeafScan(PartitionTaskLeafScanNode { task_op }) => {
            let partition_set = leaf_inputs.remove(0);
            if let VirtualPartitionSet::ScanTask(scan_tasks) = partition_set {
                PartitionTaskState::LeafScan(PartitionTaskLeafScanState::<T>::new(
                    task_op, scan_tasks,
                ))
                .into()
            } else {
                panic!(
                    "Leaf input for scan node must be scan tasks: {:?}",
                    partition_set
                )
            }
        }
        PartitionTaskNode::LeafMemory(PartitionTaskLeafMemoryNode { task_op }) => {
            let num_inputs = task_op.as_ref().map(|op| op.num_inputs()).unwrap_or(1);
            assert!(
                leaf_inputs.len() >= num_inputs,
                "task op = {:?}, num inputs = {}, num leaf inputs = {}",
                task_op.map(|op| op.name().to_string()),
                num_inputs,
                leaf_inputs.len()
            );
            let partition_sets = leaf_inputs.drain(..num_inputs);
            let part_refs = partition_sets
                .map(|p| match p {
                    VirtualPartitionSet::PartitionRef(part_refs) => part_refs,
                    VirtualPartitionSet::ScanTask(_) => panic!(
                        "Leaf input for in-memory node must be in-memory references: {:?}",
                        p
                    ),
                })
                .collect::<Vec<_>>();
            let memory_state = PartitionTaskLeafMemoryState::<T>::new(task_op.clone(), part_refs);
            if task_op.is_none() {
                // If no task op for this in-memory scan, we can immediately push all inputs into the output queue.
                // TODO(Clark): We should probably lift this into the partition task scheduler, and have it be a generic procedure of
                // identifying no-op or metadata-only tasks and directly pushing inputs into outputs.
                assert!(memory_state.inputs.len() == 1);
                assert!(memory_state.outputs.len() == 1);
                let mut input_queue = memory_state.inputs[0].borrow_mut();
                let mut output_queue = memory_state.outputs[0].borrow_mut();
                while let Some(item) = input_queue.pop_front() {
                    output_queue.push_back(item);
                }
            }
            PartitionTaskState::LeafMemory(memory_state).into()
        }
        PartitionTaskNode::Inner(PartitionTaskInnerNode { inputs, task_op }) => {
            let children = inputs
                .into_iter()
                .map(|n| task_tree_to_state_tree(n, leaf_inputs))
                .collect::<Vec<_>>();
            PartitionTaskState::Inner(PartitionTaskInnerState::new(task_op, children)).into()
        }
    }
}
