use std::{collections::VecDeque, rc::Rc};

use crate::partition::PartitionRef;

use super::op_state_tree::{InnerState, OpStateNode};

pub fn topological_sort<T: PartitionRef>(root: Rc<OpStateNode<T>>) -> Vec<Rc<OpStateNode<T>>> {
    let mut stack = VecDeque::new();
    in_order(root, &mut stack);
    let out = stack.make_contiguous();
    out.reverse();
    out.to_vec()
}

fn in_order<T: PartitionRef>(node: Rc<OpStateNode<T>>, stack: &mut VecDeque<Rc<OpStateNode<T>>>) {
    match node.as_ref() {
        OpStateNode::Inner(InnerState { children, .. }) => {
            for child in children {
                in_order(child.clone(), stack);
            }
        }
        OpStateNode::LeafScan(_) | OpStateNode::LeafMemory(_) => {}
    }
    stack.push_back(node);
}
