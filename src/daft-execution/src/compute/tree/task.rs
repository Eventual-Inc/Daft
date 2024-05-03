use std::{num::NonZeroUsize, rc::Rc};

use daft_plan::ResourceRequest;

use crate::compute::partition::{partition_task::Task, PartitionRef, PartitionTask};

use super::partition_task_state_tree::PartitionTaskState;

#[derive(Debug)]
pub struct SubmittableTask<T: PartitionRef> {
    node: Rc<PartitionTaskState<T>>,
    input_slices: Vec<NonZeroUsize>,
    resource_request: ResourceRequest,
}

impl<T: PartitionRef> SubmittableTask<T> {
    pub fn new(
        node: Rc<PartitionTaskState<T>>,
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

    pub fn finalize_for_submission(self) -> (Task<T>, RunningTask<T>) {
        let task = match self.node.as_ref() {
            PartitionTaskState::LeafScan(leaf) => {
                assert_eq!(self.input_slices.len(), 1);
                let num_inputs = self.input_slices[0].get();
                let inputs = (0..num_inputs)
                    .map(|_| leaf.inputs.borrow_mut().pop_front().unwrap().item)
                    .collect();
                Task::ScanTask(PartitionTask::new(inputs, leaf.task_op.clone()))
            }
            PartitionTaskState::LeafMemory(leaf) => {
                let inputs = self
                    .input_slices
                    .into_iter()
                    .zip(leaf.inputs.iter())
                    .map(|(num_inputs, input)| {
                        let inputs = (0..num_inputs.get())
                            .map(|_| input.borrow_mut().pop_front().unwrap().item)
                            .collect::<Vec<_>>();
                        // TODO(Clark): Support Vec<Vec<T>> inputs for bundling ops.
                        assert_eq!(inputs.len(), 1);
                        inputs.into_iter().next().unwrap()
                    })
                    .collect::<Vec<_>>();
                Task::PartitionTask(PartitionTask::new(inputs, leaf.task_op.clone().unwrap()))
            }
            PartitionTaskState::Inner(inner) => {
                let inputs = self
                    .input_slices
                    .into_iter()
                    .zip(inner.inputs.iter())
                    .map(|(num_inputs, input)| {
                        let inputs = (0..num_inputs.get())
                            .map(|_| input.borrow_mut().pop_front().unwrap().item)
                            .collect::<Vec<_>>();
                        // TODO(Clark): Support Vec<Vec<T>> inputs for bundling ops.
                        assert_eq!(inputs.len(), 1);
                        inputs.into_iter().next().unwrap()
                    })
                    .collect::<Vec<_>>();
                Task::PartitionTask(PartitionTask::new(inputs, inner.task_op.clone()))
            }
        };
        let task_id = task.task_id();
        (task, RunningTask::new(self.node, task_id))
    }
}

#[derive(Debug)]
pub struct RunningTask<T: PartitionRef> {
    pub node: Rc<PartitionTaskState<T>>,
    pub task_id: usize,
    pub output_seq_no: usize,
}

impl<T: PartitionRef> RunningTask<T> {
    fn new(node: Rc<PartitionTaskState<T>>, task_id: usize) -> Self {
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
