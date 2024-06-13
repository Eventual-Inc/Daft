use std::{num::NonZeroUsize, rc::Rc};

use daft_plan::ResourceRequest;

use crate::partition::{PartitionRef, VirtualPartition};

use crate::task::{PartitionTask, Task};
use crate::tree::OpStateNode;

pub fn all_unordered_submittable_tasks<T: PartitionRef>(
    state: Rc<OpStateNode<T>>,
) -> impl Iterator<Item = SubmittableTask<T>> {
    // TODO(Clark): Implement once we want to support out-of-order execution.
    std::iter::empty()
}

/// Get next submittable task for an operator, respecting order of input partitions.
pub fn next_in_order_submittable_task<T: PartitionRef>(
    state: Rc<OpStateNode<T>>,
) -> Option<SubmittableTask<T>> {
    match state.as_ref() {
        OpStateNode::LeafScan(scan_state) => {
            let node = state.clone();
            scan_state.inputs.borrow().front().map(|input| {
                let input_meta = vec![input.item.metadata()];
                let resource_request = scan_state
                    .task_op
                    .resource_request_with_input_metadata(&input_meta);
                let input_slices = vec![1usize.try_into().unwrap()];
                SubmittableTask::new(node, input_slices, resource_request)
            })
        }
        OpStateNode::LeafMemory(memory_state) => {
            let inputs = memory_state
                .inputs
                .iter()
                .map(|input| input.borrow().front().cloned())
                .collect::<Option<Vec<_>>>();
            let node = state.clone();
            inputs.map(|inputs| {
                // Our task tree -> state tree translation guarantees that any LeafMemory nodes with None task ops
                // (i.e. no-op in-memory scans) will have their inputs moved into their outputs before execution,
                // so we can safely unwrap the Option<PartitionTaksOp>.
                let task_op = memory_state.task_op.as_ref().unwrap().clone();
                let input_meta = inputs
                    .iter()
                    .map(|input| input.item.metadata())
                    .collect::<Vec<_>>();
                let resource_request = task_op.resource_request_with_input_metadata(&input_meta);
                let input_slices = std::iter::repeat(1usize.try_into().unwrap())
                    .take(inputs.len())
                    .collect();
                SubmittableTask::new(node, input_slices, resource_request)
            })
        }
        OpStateNode::Inner(inner_state) => {
            let inputs = inner_state
                .inputs
                .iter()
                .map(|input| input.borrow().front().cloned())
                .collect::<Option<Vec<_>>>();
            let node = state.clone();
            inputs.map(|inputs| {
                let task_op = inner_state.task_op.clone();
                let input_meta = inputs
                    .iter()
                    .map(|input| input.item.metadata())
                    .collect::<Vec<_>>();
                let resource_request = task_op.resource_request_with_input_metadata(&input_meta);
                let input_slices = std::iter::repeat(1usize.try_into().unwrap())
                    .take(inputs.len())
                    .collect();
                SubmittableTask::new(node, input_slices, resource_request)
            })
        }
    }
}

/// A lightweight submittable task spec, containing the operator and input queue slice spec for the task, along with
/// a finalized resource request.
#[derive(Debug)]
pub struct SubmittableTask<T: PartitionRef> {
    // Operator for the task.
    node: Rc<OpStateNode<T>>,
    // Slice specification for the operator's input queues.
    input_slices: Vec<NonZeroUsize>,
    // Resource request for task.
    resource_request: ResourceRequest,
}

impl<T: PartitionRef> SubmittableTask<T> {
    pub fn new(
        node: Rc<OpStateNode<T>>,
        input_slices: Vec<NonZeroUsize>,
        resource_request: ResourceRequest,
    ) -> Self {
        Self {
            node,
            input_slices,
            resource_request,
        }
    }

    pub fn op_name(&self) -> &str {
        self.node.op_name()
    }

    pub fn op_id(&self) -> usize {
        self.node.op_id()
    }

    pub fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    pub fn num_queued_outputs(&self) -> usize {
        self.node.num_queued_outputs()
    }

    /// Finalize the submittable task into a pair of a task that can be run by an executor + a scheduler-side running
    /// task state.
    pub fn finalize_for_submission(self) -> (Task<T>, RunningTask<T>) {
        let task = self
            .node
            .create_task(self.input_slices, self.resource_request);
        let task_id = task.task_id();
        (task, RunningTask::new(self.node, task_id))
    }
}

/// A scheduler-side running task state.
#[derive(Debug)]
pub struct RunningTask<T: PartitionRef> {
    // Operator state node for the task.
    pub node: Rc<OpStateNode<T>>,
    // ID of the task.
    pub task_id: usize,
    // The sequence number for the output, which the scheduler/operator state node can use to maintain partition
    // ordering.
    pub output_seq_no: usize,
}

impl<T: PartitionRef> RunningTask<T> {
    fn new(node: Rc<OpStateNode<T>>, task_id: usize) -> Self {
        let output_seq_no = node.next_output_seq_no();
        Self {
            node,
            task_id,
            output_seq_no,
        }
    }

    pub fn op_id(&self) -> usize {
        self.node.op_id()
    }

    pub fn output_seq_no(&self) -> usize {
        self.output_seq_no
    }
}
