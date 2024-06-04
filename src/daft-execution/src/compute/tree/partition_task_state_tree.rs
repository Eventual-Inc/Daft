use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::compute::{
    ops::PartitionTaskOp, partition::virtual_partition::VirtualPartition, partition::PartitionRef,
};

use super::{
    queue::{OrderedDeque, OrderedDequeItem},
    SubmittableTask,
};

static OP_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct PartitionTaskLeafScanState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = ScanTask>>,
    pub op_id: usize,
    pub inputs: Rc<RefCell<OrderedDeque<Arc<ScanTask>>>>,
    pub outputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    task_id_counter: AtomicUsize,
}

impl<T: PartitionRef> PartitionTaskLeafScanState<T> {
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
}

impl<T: PartitionRef, P> From<(P, Vec<Arc<ScanTask>>)> for PartitionTaskLeafScanState<T>
where
    P: PartitionTaskOp<Input = ScanTask> + 'static,
{
    fn from(value: (P, Vec<Arc<ScanTask>>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug)]
pub struct PartitionTaskLeafMemoryState<T: PartitionRef> {
    pub task_op: Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
    pub op_id: usize,
    pub inputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    pub outputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    task_id_counter: AtomicUsize,
}

impl<T: PartitionRef> PartitionTaskLeafMemoryState<T> {
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
}

impl<T: PartitionRef>
    From<(
        Option<Arc<dyn PartitionTaskOp<Input = MicroPartition>>>,
        Vec<Vec<T>>,
    )> for PartitionTaskLeafMemoryState<T>
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
pub struct PartitionTaskInnerState<T: PartitionRef> {
    pub task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
    pub children: Vec<Rc<PartitionTaskState<T>>>,
    pub op_id: usize,
    pub inputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    pub outputs: Vec<Rc<RefCell<OrderedDeque<T>>>>,
    task_id_counter: AtomicUsize,
}

impl<T: PartitionRef> PartitionTaskInnerState<T> {
    pub fn new(
        task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>>,
        children: Vec<Rc<PartitionTaskState<T>>>,
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
}

impl<T: PartitionRef, P> From<(P, Vec<Rc<PartitionTaskState<T>>>)> for PartitionTaskInnerState<T>
where
    P: PartitionTaskOp<Input = MicroPartition> + 'static,
{
    fn from(value: (P, Vec<Rc<PartitionTaskState<T>>>)) -> Self {
        let (task_op, inputs) = value;
        Self::new(Arc::new(task_op), inputs)
    }
}

#[derive(Debug)]
pub enum PartitionTaskState<T: PartitionRef> {
    LeafScan(PartitionTaskLeafScanState<T>),
    LeafMemory(PartitionTaskLeafMemoryState<T>),
    Inner(PartitionTaskInnerState<T>),
}

impl<T: PartitionRef> PartitionTaskState<T> {
    pub fn all_unordered_submittable_tasks(
        self: Rc<Self>,
    ) -> impl Iterator<Item = SubmittableTask<T>> {
        std::iter::empty()
    }
    pub fn next_in_order_submittable_task(self: Rc<Self>) -> Option<SubmittableTask<T>> {
        match self.as_ref() {
            Self::LeafScan(scan_state) => {
                let node = self.clone();
                scan_state.inputs.borrow().front().map(|input| {
                    let input_meta = vec![input.item.metadata()];
                    let resource_request = scan_state
                        .task_op
                        .resource_request_with_input_metadata(&input_meta);
                    let input_slices = vec![1usize.try_into().unwrap()];
                    SubmittableTask::new(node, input_slices, resource_request)
                })
            }
            Self::LeafMemory(memory_state) => {
                let inputs = memory_state
                    .inputs
                    .iter()
                    .map(|input| input.borrow().front().cloned())
                    .collect::<Option<Vec<_>>>();
                let node = self.clone();
                inputs.map(|inputs| {
                    // Our task tree -> state tree translation guarantees that any LeafMemory nodes with None task ops
                    // (i.e. no-op in-memory scans) will have their inputs moved into their outputs before execution,
                    // so we can safely unwrap the Option<PartitionTaksOp>.
                    let task_op = memory_state.task_op.as_ref().unwrap().clone();
                    let input_meta = inputs
                        .iter()
                        .map(|input| input.item.metadata())
                        .collect::<Vec<_>>();
                    let resource_request =
                        task_op.resource_request_with_input_metadata(&input_meta);
                    let input_slices = std::iter::repeat(1usize.try_into().unwrap())
                        .take(inputs.len())
                        .collect();
                    SubmittableTask::new(node, input_slices, resource_request)
                })
            }
            Self::Inner(inner_state) => {
                let inputs = inner_state
                    .inputs
                    .iter()
                    .map(|input| input.borrow().front().cloned())
                    .collect::<Option<Vec<_>>>();
                let node = self.clone();
                inputs.map(|inputs| {
                    let task_op = inner_state.task_op.clone();
                    let input_meta = inputs
                        .iter()
                        .map(|input| input.item.metadata())
                        .collect::<Vec<_>>();
                    let resource_request =
                        task_op.resource_request_with_input_metadata(&input_meta);
                    let input_slices = std::iter::repeat(1usize.try_into().unwrap())
                        .take(inputs.len())
                        .collect();
                    SubmittableTask::new(node, input_slices, resource_request)
                })
            }
        }
    }
    pub fn push_outputs_back(&self, new_outputs: Vec<T>, output_seq_no: usize) {
        match self {
            Self::LeafScan(PartitionTaskLeafScanState { outputs, .. })
            | Self::LeafMemory(PartitionTaskLeafMemoryState { outputs, .. })
            | Self::Inner(PartitionTaskInnerState { outputs, .. }) => outputs
                .iter()
                .zip(new_outputs)
                .for_each(|(out, new_out)| out.borrow_mut().push_back((output_seq_no, new_out))),
        }
    }

    pub fn pop_outputs(&self) -> Option<Vec<T>> {
        match self {
            Self::LeafScan(PartitionTaskLeafScanState { outputs, .. })
            | Self::LeafMemory(PartitionTaskLeafMemoryState { outputs, .. })
            | Self::Inner(PartitionTaskInnerState { outputs, .. }) => outputs
                .iter()
                .map(|v| v.borrow_mut().pop_front().map(|item| item.item))
                .collect(),
        }
    }

    fn outputs(&self) -> Vec<Rc<RefCell<OrderedDeque<T>>>> {
        match self {
            Self::LeafScan(PartitionTaskLeafScanState { outputs, .. })
            | Self::LeafMemory(PartitionTaskLeafMemoryState { outputs, .. })
            | Self::Inner(PartitionTaskInnerState { outputs, .. }) => outputs.clone(),
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
            Self::LeafScan(PartitionTaskLeafScanState { outputs, .. })
            | Self::LeafMemory(PartitionTaskLeafMemoryState { outputs, .. })
            | Self::Inner(PartitionTaskInnerState { outputs, .. }) => outputs
                .iter()
                .map(|output_lane| output_lane.borrow().len())
                .sum(),
        }
    }

    pub fn next_output_seq_no(&self) -> usize {
        match self {
            Self::LeafScan(PartitionTaskLeafScanState {
                task_id_counter, ..
            })
            | Self::LeafMemory(PartitionTaskLeafMemoryState {
                task_id_counter, ..
            })
            | Self::Inner(PartitionTaskInnerState {
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
