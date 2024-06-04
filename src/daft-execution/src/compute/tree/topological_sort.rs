use std::{collections::VecDeque, rc::Rc};

use crate::compute::partition::PartitionRef;

use super::partition_task_state_tree::{PartitionTaskInnerState, PartitionTaskState};

pub fn topological_sort<T: PartitionRef>(
    root: Rc<PartitionTaskState<T>>,
) -> Vec<Rc<PartitionTaskState<T>>> {
    let mut stack = VecDeque::new();
    in_order(root, &mut stack);
    let out = stack.make_contiguous();
    out.reverse();
    out.to_vec()
}

fn in_order<T: PartitionRef>(
    node: Rc<PartitionTaskState<T>>,
    stack: &mut VecDeque<Rc<PartitionTaskState<T>>>,
) {
    match node.as_ref() {
        PartitionTaskState::Inner(PartitionTaskInnerState { children, .. }) => {
            for child in children {
                in_order(child.clone(), stack);
            }
        }
        PartitionTaskState::LeafScan(_) | PartitionTaskState::LeafMemory(_) => {}
    }
    stack.push_back(node);
}
