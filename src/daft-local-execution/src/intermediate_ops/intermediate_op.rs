use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_runtime, RuntimeRef};
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{
        create_channel, create_ordering_aware_receiver_channel, OrderingAwareReceiver, Receiver,
        Sender,
    },
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext, OperatorOutput, NUM_CPUS,
};

pub(crate) trait IntermediateOpState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

struct DefaultIntermediateOperatorState {}
impl IntermediateOpState for DefaultIntermediateOperatorState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
}

pub(crate) type IntermediateOpExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)>>;
pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult;
    fn name(&self) -> &'static str;
    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(DefaultIntermediateOperatorState {}))
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        } else {
            Arc::new(UnorderedDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        }
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
        receiver: Receiver<Arc<MicroPartition>>,
        sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<()> {
        let span = info_span!("IntermediateOp::execute");
        let compute_runtime = get_compute_runtime();
        let mut state = op.make_state()?;
        while let Some(morsel) = receiver.recv().await {
            loop {
                let result = rt_context
                    .in_span(&span, || {
                        op.execute(morsel.clone(), state, &compute_runtime)
                    })
                    .await??;
                state = result.0;
                match result.1 {
                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                        if sender.send(mp).await.is_err() {
                            return Ok(());
                        }
                        break;
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        break;
                    }
                    IntermediateOperatorResult::HasMoreOutput(mp) => {
                        if sender.send(mp).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn spawn_workers(
        &self,
        input_receivers: Vec<Receiver<Arc<MicroPartition>>>,
        runtime_handle: &mut ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> OrderingAwareReceiver<Arc<MicroPartition>> {
        let (output_sender, output_receiver) =
            create_ordering_aware_receiver_channel(maintain_order, input_receivers.len());
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_sender) {
            runtime_handle.spawn(
                Self::run_worker(
                    self.intermediate_op.clone(),
                    input_receiver,
                    output_sender,
                    self.runtime_stats.clone(),
                ),
                self.intermediate_op.name(),
            );
        }
        output_receiver
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
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(CountingReceiver::new(
                child_result_receiver,
                self.runtime_stats.clone(),
            ));
        }
        let op = self.intermediate_op.clone();
        let num_workers = op.max_concurrency();
        let (destination_sender, destination_receiver) = create_channel(1);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());

        let dispatch_spawner = self
            .intermediate_op
            .dispatch_spawner(runtime_handle, maintain_order);
        let spawned_dispatch_result = dispatch_spawner.spawn_dispatch(
            child_result_receivers,
            num_workers,
            &mut runtime_handle.handle(),
        );
        runtime_handle.spawn(
            async move { spawned_dispatch_result.spawned_dispatch_task.await? },
            self.name(),
        );

        let mut output_receiver = self.spawn_workers(
            spawned_dispatch_result.worker_receivers,
            runtime_handle,
            maintain_order,
        );
        runtime_handle.spawn(
            async move {
                while let Some(morsel) = output_receiver.recv().await {
                    if counting_sender.send(morsel).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            },
            op.name(),
        );
        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
