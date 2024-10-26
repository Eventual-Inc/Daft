use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{
        create_channel, create_ordering_aware_receiver_channel,
        create_ordering_aware_sender_channel, OrderingAwareSender, Receiver, Sender,
    },
    dispatcher::dispatch,
    pipeline::PipelineNode,
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

#[async_trait]
pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        idx: usize,
        input: &Arc<MicroPartition>,
        state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult>;
    fn name(&self) -> &'static str;
    async fn make_state(&self) -> Box<dyn DynIntermediateOpState> {
        Box::new(DefaultIntermediateOperatorState {})
    }
    fn morsel_size(&self) -> Option<usize> {
        Some(DaftExecutionConfig::default().default_morsel_size)
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
        receiver: Receiver<(usize, Arc<MicroPartition>)>,
        sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<()> {
        let span = info_span!("IntermediateOp::execute");
        let compute_runtime = get_compute_runtime();
        let state_wrapper = IntermediateOperatorState::new(op.make_state().await);
        while let Ok((idx, morsel)) = receiver.recv_async().await {
            loop {
                let op = op.clone();
                let morsel = morsel.clone();
                let span = span.clone();
                let rt_context = rt_context.clone();
                let state_wrapper = state_wrapper.clone();
                let fut = async move {
                    rt_context.in_span(&span, || op.execute(idx, &morsel, &state_wrapper))
                };
                let result = compute_runtime.await_on(fut).await??;
                match result {
                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                        let _ = sender.send_async(mp).await;
                        break;
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        break;
                    }
                    IntermediateOperatorResult::HasMoreOutput(mp) => {
                        let _ = sender.send_async(mp).await;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn spawn_workers(
        &self,
        num_workers: usize,
        output_senders: Vec<Sender<Arc<MicroPartition>>>,
        runtime_handle: &mut ExecutionRuntimeHandle,
        maintain_order: bool,
    ) -> OrderingAwareSender<(usize, Arc<MicroPartition>)> {
        let (worker_sender, worker_receivers) =
            create_ordering_aware_sender_channel(maintain_order, num_workers);
        for (receiver, destination_channel) in worker_receivers.into_iter().zip(output_senders) {
            runtime_handle.spawn(
                Self::run_worker(
                    self.intermediate_op.clone(),
                    receiver,
                    destination_channel,
                    self.runtime_stats.clone(),
                ),
                self.intermediate_op.name(),
            );
        }
        worker_sender
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
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let num_workers = *NUM_CPUS;
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(CountingReceiver::new(
                child_result_receiver,
                self.runtime_stats.clone(),
            ));
        }
        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let (output_senders, mut output_receiver) =
            create_ordering_aware_receiver_channel(maintain_order, num_workers);
        let worker_sender =
            self.spawn_workers(num_workers, output_senders, runtime_handle, maintain_order);
        let morsel_size = runtime_handle.determine_morsel_size(self.intermediate_op.morsel_size());
        runtime_handle.spawn(
            dispatch(child_result_receivers, worker_sender, morsel_size),
            self.intermediate_op.name(),
        );
        runtime_handle.spawn(
            async move {
                while let Some(val) = output_receiver.recv().await {
                    let _ = counting_sender.send(val).await;
                }
                Ok(())
            },
            self.intermediate_op.name(),
        );
        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
