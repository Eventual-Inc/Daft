use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::ScanTask;
use std::{
    cell::RefCell,
    num::NonZeroUsize,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    ops::PartitionTaskOp,
    partition::{virtual_partition::VirtualPartition, PartitionRef},
    task::{PartitionTask, Task},
};

use super::queue::{OrderedDeque, OrderedDequeItem};

pub(super) static OP_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct LeafScanState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
    pub op_id: usize,
    pub inputs: Rc<RefCell<OrderedDeque<Arc<ScanTask>>>>,
    pub outputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    task_id_counter: AtomicUsize,
}

impl<T: PartitionRef> LeafScanState<T> {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
        inputs: Vec<Arc<ScanTask>>,
    ) -> Self {
        let inputs = Rc::new(RefCell::new(OrderedDeque::from(
            inputs.into_iter().enumerate().map(OrderedDequeItem::from),
        )));
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = std::iter::repeat_with(|| Rc::new(RefCell::new(OrderedDeque::new())))
            .take(task_op.num_outputs())
            .collect::<Vec<_>>();
        let task_id_counter = AtomicUsize::new(0);
        Self {
            task_op,
            op_id,
            inputs,
            outputs,
            task_id_counter,
        }
    }

    pub fn pop_inputs(&self, input_slices: Vec<NonZeroUsize>) -> Vec<Arc<ScanTask>> {
        assert_eq!(input_slices.len(), 1);
        let num_inputs = input_slices[0].get();
        let mut inputs = self.inputs.borrow_mut();
        (0..num_inputs)
            .map(|_| inputs.pop_front().unwrap().item)
            .collect()
    }
}

impl<T: PartitionRef, P> From<(P, Vec<Arc<ScanTask>>)> for LeafScanState<T>
where
    P: PartitionTaskOp<Input = ScanTask> + 'static,
{
    fn from(value: (P, Vec<Arc<ScanTask>>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug)]
pub struct LeafMemoryState<T: PartitionRef> {
    pub task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    pub op_id: usize,
    pub inputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    pub outputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    task_id_counter: AtomicUsize,
}

impl<T: PartitionRef> LeafMemoryState<T> {
    pub fn new(
        task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        inputs: Vec<Vec<T>>,
    ) -> Self {
        let inputs = inputs
            .into_iter()
            .map(|input| {
                Rc::new(RefCell::new(OrderedDeque::from(
                    input.into_iter().enumerate().map(OrderedDequeItem::from),
                )))
            })
            .collect::<Vec<_>>();
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = std::iter::repeat_with(|| Rc::new(RefCell::new(OrderedDeque::new())))
            .take(task_op.as_ref().map_or(1, |op| op.num_outputs()))
            .collect::<Vec<_>>();
        let task_id_counter = AtomicUsize::new(0);
        Self {
            task_op,
            op_id,
            inputs,
            outputs,
            task_id_counter,
        }
    }

    pub fn pop_inputs(&self, input_slices: Vec<NonZeroUsize>) -> Vec<T> {
        input_slices
            .into_iter()
            .zip(self.inputs.iter())
            .map(|(num_inputs, input)| {
                let mut input = input.borrow_mut();
                let inputs = (0..num_inputs.get())
                    .map(|_| input.pop_front().unwrap().item)
                    .collect::<Vec<_>>();
                // TODO(Clark): Support Vec<Vec<T>> inputs for bundling ops.
                assert_eq!(inputs.len(), 1);
                inputs.into_iter().next().unwrap()
            })
            .collect()
    }
}

impl<T: PartitionRef>
    From<(
        Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        Vec<Vec<T>>,
    )> for LeafMemoryState<T>
{
    fn from(
        value: (
            Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
            Vec<Vec<T>>,
        ),
    ) -> Self {
        let (task_op, inputs) = value;
        Self::new(task_op, inputs)
    }
}

#[derive(Debug)]
pub struct InnerState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
    pub children: Vec<Rc<OpStateNode<T>>>,
    pub op_id: usize,
    pub inputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    pub outputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    task_id_counter: AtomicUsize,
}

impl<T: PartitionRef> InnerState<T> {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
        children: Vec<Rc<OpStateNode<T>>>,
    ) -> Self {
        let inputs = children
            .iter()
            .map(|child| {
                let child_outputs = child.outputs();
                assert!(child_outputs.len() == 1);
                child_outputs.into_iter().next().unwrap()
            })
            .collect::<Vec<_>>();
        let op_id = OP_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let outputs = std::iter::repeat_with(|| Rc::new(RefCell::new(OrderedDeque::new())))
            .take(task_op.num_outputs())
            .collect::<Vec<_>>();
        let task_id_counter = AtomicUsize::new(0);
        Self {
            task_op,
            children,
            op_id,
            inputs,
            outputs,
            task_id_counter,
        }
    }

    pub fn pop_inputs(&self, input_slices: Vec<NonZeroUsize>) -> Vec<T> {
        input_slices
            .into_iter()
            .zip(self.inputs.iter())
            .map(|(num_inputs, input)| {
                let mut input = input.borrow_mut();
                let inputs = (0..num_inputs.get())
                    .map(|_| input.pop_front().unwrap().item)
                    .collect::<Vec<_>>();
                // TODO(Clark): Support Vec<Vec<T>> inputs for bundling ops.
                assert_eq!(inputs.len(), 1);
                inputs.into_iter().next().unwrap()
            })
            .collect()
    }
}

impl<T: PartitionRef, P> From<(P, Vec<Rc<OpStateNode<T>>>)> for InnerState<T>
where
    P: PartitionTaskOp<Input = MicroPartition> + 'static,
{
    fn from(value: (P, Vec<Rc<OpStateNode<T>>>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug)]
pub enum OpStateNode<T: PartitionRef> {
    LeafScan(LeafScanState<T>),
    LeafMemory(LeafMemoryState<T>),
    Inner(InnerState<T>),
}

impl<T: PartitionRef> OpStateNode<T> {
    pub fn push_outputs_back(&self, new_outputs: Vec<T>, output_seq_no: usize) {
        match self {
            Self::LeafScan(LeafScanState { outputs, .. })
            | Self::LeafMemory(LeafMemoryState { outputs, .. })
            | Self::Inner(InnerState { outputs, .. }) => outputs
                .iter()
                .zip(new_outputs)
                .for_each(|(out, new_out)| out.borrow_mut().push_back((output_seq_no, new_out))),
        }
    }

    pub fn pop_outputs(&self) -> Option<Vec<T>> {
        match self {
            Self::LeafScan(LeafScanState { outputs, .. })
            | Self::LeafMemory(LeafMemoryState { outputs, .. })
            | Self::Inner(InnerState { outputs, .. }) => outputs
                .iter()
                .map(|v| v.borrow_mut().pop_front().map(|item| item.item))
                .collect(),
        }
    }

    pub fn create_task(
        &self,
        input_slices: Vec<NonZeroUsize>,
        resource_request: ResourceRequest,
    ) -> Task<T> {
        match self {
            OpStateNode::LeafScan(leaf) => Task::ScanTask(PartitionTask::new(
                leaf.pop_inputs(input_slices),
                leaf.task_op.clone(),
                resource_request,
            )),
            OpStateNode::LeafMemory(leaf) => Task::PartitionTask(PartitionTask::new(
                leaf.pop_inputs(input_slices),
                leaf.task_op.clone().unwrap(),
                resource_request,
            )),
            OpStateNode::Inner(inner) => Task::PartitionTask(PartitionTask::new(
                inner.pop_inputs(input_slices),
                inner.task_op.clone(),
                resource_request,
            )),
        }
    }

    fn outputs(&self) -> Vec<Rc<RefCell<OrderedDeque<T>>>> {
        match self {
            Self::LeafScan(LeafScanState { outputs, .. })
            | Self::LeafMemory(LeafMemoryState { outputs, .. })
            | Self::Inner(InnerState { outputs, .. }) => outputs.clone(),
        }
    }

    pub fn num_queued_inputs(&self) -> usize {
        match self {
            Self::LeafScan(leaf) => leaf.inputs.borrow().len(),
            Self::LeafMemory(leaf) => leaf.inputs[0].borrow().len(),
            Self::Inner(inner) => inner.inputs[0].borrow().len(),
        }
    }

    pub fn num_queued_outputs(&self) -> usize {
        match self {
            Self::LeafScan(LeafScanState { outputs, .. })
            | Self::LeafMemory(LeafMemoryState { outputs, .. })
            | Self::Inner(InnerState { outputs, .. }) => outputs[0].borrow().len(),
        }
    }

    pub fn next_output_seq_no(&self) -> usize {
        match self {
            Self::LeafScan(LeafScanState {
                task_id_counter, ..
            })
            | Self::LeafMemory(LeafMemoryState {
                task_id_counter, ..
            })
            | Self::Inner(InnerState {
                task_id_counter, ..
            }) => task_id_counter.fetch_add(1, Ordering::SeqCst),
        }
    }

    pub fn op_id(&self) -> usize {
        match self {
            Self::LeafScan(leaf) => leaf.op_id,
            Self::LeafMemory(leaf) => leaf.op_id,
            Self::Inner(inner) => inner.op_id,
        }
    }

    pub fn op_name(&self) -> &str {
        match self {
            Self::LeafScan(leaf) => leaf.task_op.name(),
            Self::LeafMemory(leaf) => leaf
                .task_op
                .as_ref()
                .map(|op| op.name())
                .unwrap_or("InMemoryScan"),
            Self::Inner(inner) => inner.task_op.name(),
        }
    }
}
#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::schema::Schema;
    use daft_plan::ResourceRequest;
    use daft_scan::{
        file_format::FileFormatConfig, storage_config::StorageConfig, DataFileSource, ScanTask,
    };
    use rstest::rstest;
    use std::{
        assert_matches::assert_matches,
        num::NonZeroUsize,
        rc::Rc,
        sync::{atomic::Ordering, Arc},
    };

    use crate::{
        executor::local::local_partition_ref::LocalPartitionRef,
        partition::PartitionRef,
        task::Task,
        test::{mock_micropartition, mock_scan_task, MockInputOutputOp, MockOutputOp},
    };

    use super::{InnerState, LeafMemoryState, LeafScanState, OpStateNode, OP_ID_COUNTER};

    /// Test leaf scan state node.
    #[rstest]
    fn leaf_scan(#[values(1, 2, 10)] num_outputs: usize) -> DaftResult<()> {
        let op = MockOutputOp::<ScanTask>::new("op", num_outputs);
        let scan_task = mock_scan_task();
        let node = OpStateNode::LeafScan(LeafScanState::<LocalPartitionRef>::new(
            Arc::new(op),
            vec![Arc::new(scan_task)],
        ));
        assert_eq!(node.op_name(), "op");
        assert_eq!(node.num_queued_inputs(), 1);

        // Test task creation.
        let resource_request = ResourceRequest::new_internal(Some(1.0), Some(1.0), Some(1024));
        let task = node.create_task(
            vec![NonZeroUsize::new(1).unwrap()],
            resource_request.clone(),
        );
        // Check that scan task was created.
        assert_matches!(task, Task::ScanTask(pt) if pt.resource_request() == &resource_request);
        // Check that input has been popped.
        assert_eq!(node.num_queued_inputs(), 0);

        // Test output queueing.
        assert_eq!(node.num_queued_outputs(), 0);
        let seq_no1 = node.next_output_seq_no();
        assert_eq!(seq_no1, 0);
        let output1 = Arc::new(mock_micropartition(1));
        let seq_no2 = node.next_output_seq_no();
        assert_eq!(seq_no2, 1);
        let output2 = Arc::new(mock_micropartition(2));
        // Add second output before first output.
        node.push_outputs_back(
            std::iter::repeat_with(|| LocalPartitionRef::try_new(output2.clone()).unwrap())
                .take(num_outputs)
                .collect(),
            seq_no2,
        );
        assert_eq!(node.num_queued_outputs(), 1);
        // Missing first output, so pop_outputs() should return None.
        assert_matches!(node.pop_outputs(), None);
        // Add first output.
        node.push_outputs_back(
            std::iter::repeat_with(|| LocalPartitionRef::try_new(output1.clone()).unwrap())
                .take(num_outputs)
                .collect(),
            seq_no1,
        );
        assert_eq!(node.num_queued_outputs(), 2);

        // Pop outputs and check for expected ordering + format.
        assert_matches!(node.pop_outputs(), Some(v) if v.len() == num_outputs && v[0].partition().len() == 1);
        assert_eq!(node.num_queued_outputs(), 1);
        assert_matches!(node.pop_outputs(), Some(v) if v.len() == num_outputs && v[0].partition().len() == 2);
        assert_eq!(node.num_queued_outputs(), 0);
        Ok(())
    }

    /// Test leaf memory state node.
    #[rstest]
    fn leaf_memory(
        #[values(1, 2, 10)] num_inputs: usize,
        #[values(1, 2, 10)] num_outputs: usize,
    ) -> DaftResult<()> {
        let op = MockInputOutputOp::new("op", num_inputs, num_outputs);
        let input = Arc::new(mock_micropartition(1));
        let num_partitions_per_lane = 2;
        let node = OpStateNode::LeafMemory(LeafMemoryState::<LocalPartitionRef>::new(
            Some(Arc::new(op)),
            std::iter::repeat_with(|| {
                std::iter::repeat_with(|| LocalPartitionRef::try_new(input.clone()).unwrap())
                    .take(num_partitions_per_lane)
                    .collect::<Vec<_>>()
            })
            .take(num_inputs)
            .collect(),
        ));
        assert_eq!(node.op_name(), "op");
        assert_eq!(node.num_queued_inputs(), num_partitions_per_lane);

        // Test task creation.
        let input_slice = std::iter::repeat(NonZeroUsize::new(1).unwrap())
            .take(num_inputs)
            .collect::<Vec<_>>();
        let resource_request = ResourceRequest::new_internal(Some(1.0), Some(1.0), Some(1024));
        // Create task for each input.
        for i in 1..num_partitions_per_lane + 1 {
            let task = node.create_task(input_slice.clone(), resource_request.clone());
            // Check that scan task was created.
            assert_matches!(task, Task::PartitionTask(pt) if pt.resource_request() == &resource_request);
            // Check that input has been popped.
            assert_eq!(node.num_queued_inputs(), num_partitions_per_lane - i);
        }

        // Test output queueing.
        assert_eq!(node.num_queued_outputs(), 0);
        let seq_no1 = node.next_output_seq_no();
        assert_eq!(seq_no1, 0);
        let output1 = Arc::new(mock_micropartition(1));
        let seq_no2 = node.next_output_seq_no();
        assert_eq!(seq_no2, 1);
        let output2 = Arc::new(mock_micropartition(2));
        // Add second output before first output.
        node.push_outputs_back(
            std::iter::repeat_with(|| LocalPartitionRef::try_new(output2.clone()).unwrap())
                .take(num_outputs)
                .collect(),
            seq_no2,
        );
        assert_eq!(node.num_queued_outputs(), 1);
        // Missing first output, so pop_outputs() should return None.
        assert_matches!(node.pop_outputs(), None);
        // Add first output.
        node.push_outputs_back(
            std::iter::repeat_with(|| LocalPartitionRef::try_new(output1.clone()).unwrap())
                .take(num_outputs)
                .collect(),
            seq_no1,
        );
        assert_eq!(node.num_queued_outputs(), 2);

        // Pop outputs and check for expected ordering + format.
        assert_matches!(node.pop_outputs(), Some(v) if v.len() == num_outputs && v[0].partition().len() == 1);
        assert_eq!(node.num_queued_outputs(), 1);
        assert_matches!(node.pop_outputs(), Some(v) if v.len() == num_outputs && v[0].partition().len() == 2);
        assert_eq!(node.num_queued_outputs(), 0);
        Ok(())
    }

    /// Test inner state node.
    #[rstest]
    fn inner(
        #[values(1, 2, 10)] num_inputs: usize,
        #[values(1, 2, 10)] num_outputs: usize,
    ) -> DaftResult<()> {
        // Upstream/children configuration.
        let upstream_op = Arc::new(MockInputOutputOp::new("upstream_op", 1, 1));
        let input = Arc::new(mock_micropartition(1));
        let children = std::iter::repeat_with(|| {
            Rc::new(OpStateNode::LeafMemory(
                LeafMemoryState::<LocalPartitionRef>::new(
                    Some(upstream_op.clone()),
                    vec![vec![LocalPartitionRef::try_new(input.clone()).unwrap()]],
                ),
            ))
        })
        .take(num_inputs)
        .collect::<Vec<_>>();

        // Inner node configuration.
        let op = Arc::new(MockInputOutputOp::new("op", num_inputs, num_outputs));
        let num_partitions_per_lane = 2;
        let inner_state = InnerState::<LocalPartitionRef>::new(op.clone(), children.clone());
        // Check that input queues for the inner node point to the output queues of the children.
        assert_eq!(inner_state.inputs.len(), children.len());
        for (input, output) in inner_state.inputs.iter().zip(
            children
                .iter()
                .map(|child| child.outputs().into_iter().next().unwrap()),
        ) {
            assert!(Rc::ptr_eq(input, &output));
        }
        let node = OpStateNode::Inner(inner_state);
        assert_eq!(node.op_name(), "op");

        // Input queue is empty until children materialize partition references to their output queues.
        assert_eq!(node.num_queued_inputs(), 0);

        // Materialize partition references to children's output queues.
        for child in children.iter() {
            assert_eq!(child.num_queued_outputs(), 0);
            for i in 1..num_partitions_per_lane + 1 {
                let seq_no = child.next_output_seq_no();
                let output = Arc::new(mock_micropartition(i));
                child.push_outputs_back(vec![LocalPartitionRef::try_new(output).unwrap()], seq_no);
                assert_eq!(child.num_queued_outputs(), i);
            }
            assert_eq!(child.num_queued_outputs(), num_partitions_per_lane);
        }
        // Now that children have pushed partition references to their output queues (which are linked to the inner
        // node's input queues), the inner node should contain queued inputs.
        assert_eq!(node.num_queued_inputs(), num_partitions_per_lane);

        // Test task creation.
        let input_slice = std::iter::repeat(NonZeroUsize::new(1).unwrap())
            .take(num_inputs)
            .collect::<Vec<_>>();
        let resource_request = ResourceRequest::new_internal(Some(1.0), Some(1.0), Some(1024));
        // Create task for each input partition set (one partition per lane).
        for i in 1..num_partitions_per_lane + 1 {
            let task = node.create_task(input_slice.clone(), resource_request.clone());
            // Check that scan task was created.
            assert_matches!(task, Task::PartitionTask(pt) if pt.resource_request() == &resource_request);
            // Check that input has been popped.
            assert_eq!(node.num_queued_inputs(), num_partitions_per_lane - i);
            // Since the inner node's input queues are linked to their children's output queues,
            // we should see that the partition references were popped from the children's output queues
            // as well.
            for child in children.iter() {
                assert_eq!(child.num_queued_outputs(), num_partitions_per_lane - i);
            }
        }
        // All children's output queues should be empty if this inner node's input queues are empty.
        for child in children.iter() {
            assert_eq!(child.num_queued_outputs(), 0);
        }

        // Test output queueing.
        assert_eq!(node.num_queued_outputs(), 0);
        let seq_no1 = node.next_output_seq_no();
        assert_eq!(seq_no1, 0);
        let output1 = Arc::new(mock_micropartition(1));
        let seq_no2 = node.next_output_seq_no();
        assert_eq!(seq_no2, 1);
        let output2 = Arc::new(mock_micropartition(2));
        // Add second output before first output.
        node.push_outputs_back(
            std::iter::repeat_with(|| LocalPartitionRef::try_new(output2.clone()).unwrap())
                .take(num_outputs)
                .collect(),
            seq_no2,
        );
        assert_eq!(node.num_queued_outputs(), 1);
        // Missing first output, so pop_outputs() should return None.
        assert_matches!(node.pop_outputs(), None);
        // Add first output.
        node.push_outputs_back(
            std::iter::repeat_with(|| LocalPartitionRef::try_new(output1.clone()).unwrap())
                .take(num_outputs)
                .collect(),
            seq_no1,
        );
        assert_eq!(node.num_queued_outputs(), 2);

        // Pop outputs and check for expected ordering + format.
        assert_matches!(node.pop_outputs(), Some(v) if v.len() == num_outputs && v[0].partition().len() == 1);
        assert_eq!(node.num_queued_outputs(), 1);
        assert_matches!(node.pop_outputs(), Some(v) if v.len() == num_outputs && v[0].partition().len() == 2);
        assert_eq!(node.num_queued_outputs(), 0);
        Ok(())
    }
}
