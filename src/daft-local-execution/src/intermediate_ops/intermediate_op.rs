use std::sync::{Arc, Mutex};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    pipeline::{PipelineNode, PipelineResultType},
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeHandle, NUM_CPUS,
};

pub(crate) trait DynIntermediateOpState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

struct DefaultIntermediateOperatorState {}
impl DynIntermediateOpState for DefaultIntermediateOperatorState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) struct IntermediateOperatorState {
    inner: Mutex<Box<dyn DynIntermediateOpState>>,
}

impl IntermediateOperatorState {
    fn new(inner: Box<dyn DynIntermediateOpState>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(inner),
        })
    }

    pub(crate) fn with_state_mut<T: DynIntermediateOpState + 'static, F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.inner.lock().unwrap();
        let state = guard
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("State type mismatch");
        f(state)
    }
}

pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
}

pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult>;
    fn name(&self) -> &'static str;
    fn make_state(&self) -> DaftResult<Box<dyn DynIntermediateOpState>> {
        Ok(Box::new(DefaultIntermediateOperatorState {}))
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    /// The input morsel size expected by this operator. If None, use the default size.
    fn morsel_size(&self) -> Option<usize> {
        None
    }
}

pub struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
    ) -> Self {
        let rts = RuntimeStatsContext::new();
        Self::new_with_runtime_stats(intermediate_op, children, rts)
    }

    pub(crate) fn new_with_runtime_stats(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        runtime_stats: Arc<RuntimeStatsContext>,
    ) -> Self {
        Self {
            intermediate_op,
            children,
            runtime_stats,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "IntermediateOperator::run_worker")]
    pub async fn run_worker(
        op: Arc<dyn IntermediateOperator>,
        mut receiver: Receiver<(usize, PipelineResultType)>,
        sender: CountingSender,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<()> {
        let span = info_span!("IntermediateOp::execute");
        let compute_runtime = get_compute_runtime();
        let state_wrapper = IntermediateOperatorState::new(op.make_state()?);
        while let Some((idx, morsel)) = receiver.recv().await {
            loop {
                let op = op.clone();
                let morsel = morsel.clone();
                let span = span.clone();
                let rt_context = rt_context.clone();
                let state_wrapper = state_wrapper.clone();
                let fut = async move {
                    rt_context.in_span(&span, || op.execute(idx, &morsel, &state_wrapper))
                };
                let result = compute_runtime.spawn(fut).await??;
                match result {
                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                        let _ = sender.send(mp.into()).await;
                        break;
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        break;
                    }
                    IntermediateOperatorResult::HasMoreOutput(mp) => {
                        let _ = sender.send(mp.into()).await;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn spawn_workers(
        &self,
        num_workers: usize,
        destination_channel: &mut PipelineChannel,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> Vec<Sender<(usize, PipelineResultType)>> {
        let mut worker_senders = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (worker_sender, worker_receiver) = create_channel(1);
            let destination_sender =
                destination_channel.get_next_sender_with_stats(&self.runtime_stats);
            runtime_handle.spawn(
                Self::run_worker(
                    self.intermediate_op.clone(),
                    worker_receiver,
                    destination_sender,
                    self.runtime_stats.clone(),
                ),
                self.intermediate_op.name(),
            );
            worker_senders.push(worker_sender);
        }
        worker_senders
    }

    pub async fn send_to_workers(
        receivers: Vec<CountingReceiver>,
        worker_senders: Vec<Sender<(usize, PipelineResultType)>>,
        morsel_size: usize,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |idx, data: PipelineResultType| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send((idx, data))
        };

        for (idx, mut receiver) in receivers.into_iter().enumerate() {
            let mut buffer = RowBasedBuffer::new(morsel_size);
            while let Some(morsel) = receiver.recv().await {
                if morsel.should_broadcast() {
                    for worker_sender in &worker_senders {
                        let _ = worker_sender.send((idx, morsel.clone())).await;
                    }
                } else {
                    buffer.push(morsel.as_data());
                    if let Some(ready) = buffer.pop_enough()? {
                        for part in ready {
                            let _ = send_to_next_worker(idx, part.into()).await;
                        }
                    }
                }
            }
            if let Some(ready) = buffer.pop_all()? {
                let _ = send_to_next_worker(idx, ready.into()).await;
            }
        }
        Ok(())
    }
}

impl TreeDisplay for IntermediateNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.intermediate_op.name()).unwrap();
        use common_display::DisplayLevel::Compact;
        if matches!(level, Compact) {
        } else {
            let rt_result = self.runtime_stats.result();
            rt_result.display(&mut display, true, true, true).unwrap();
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children.iter().map(|v| v.as_tree_display()).collect()
    }
}

impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn name(&self) -> &'static str {
        self.intermediate_op.name()
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let child_result_channel = child.start(maintain_order, runtime_handle)?;
            child_result_receivers
                .push(child_result_channel.get_receiver_with_stats(&self.runtime_stats));
        }
        let op = self.intermediate_op.clone();
        let num_workers = op.max_concurrency();
        let mut destination_channel = PipelineChannel::new(num_workers, maintain_order);

        let worker_senders =
            self.spawn_workers(num_workers, &mut destination_channel, runtime_handle);
        runtime_handle.spawn(
            Self::send_to_workers(
                child_result_receivers,
                worker_senders,
                op.morsel_size()
                    .unwrap_or_else(|| runtime_handle.default_morsel_size()),
            ),
            op.name(),
        );
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
