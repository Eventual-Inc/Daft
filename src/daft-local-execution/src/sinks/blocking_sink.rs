use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, PipelineChannel, Receiver},
    dispatcher::{Dispatcher, RoundRobinBufferedDispatcher},
    pipeline::{PipelineNode, PipelineResultType},
    runtime_stats::RuntimeStatsContext,
    ExecutionRuntimeHandle, JoinSnafu, TaskSet,
};
pub trait BlockingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub enum BlockingSinkStatus {
    NeedMoreInput(Box<dyn BlockingSinkState>),
    #[allow(dead_code)]
    Finished(Box<dyn BlockingSinkState>),
}

pub trait BlockingSink: Send + Sync {
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: Box<dyn BlockingSinkState>,
    ) -> DaftResult<BlockingSinkStatus>;
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>>;
    fn name(&self) -> &'static str;
    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>>;
    fn make_dispatcher(&self, runtime_handle: &ExecutionRuntimeHandle) -> Arc<dyn Dispatcher> {
        Arc::new(RoundRobinBufferedDispatcher::new(
            runtime_handle.default_morsel_size(),
        ))
    }
    fn max_concurrency(&self) -> usize;
}

pub struct BlockingSinkNode {
    op: Arc<dyn BlockingSink>,
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl BlockingSinkNode {
    pub(crate) fn new(op: Arc<dyn BlockingSink>, child: Box<dyn PipelineNode>) -> Self {
        let name = op.name();
        Self {
            op,
            name,
            child,
            runtime_stats: RuntimeStatsContext::new(),
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "BlockingSink::run_worker")]
    async fn run_worker(
        op: Arc<dyn BlockingSink>,
        mut input_receiver: Receiver<PipelineResultType>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<Box<dyn BlockingSinkState>> {
        let span = info_span!("BlockingSink::Sink");
        let compute_runtime = get_compute_runtime();
        let mut state = op.make_state()?;
        while let Some(morsel) = input_receiver.recv().await {
            let op = op.clone();
            let morsel = morsel.clone();
            let span = span.clone();
            let rt_context = rt_context.clone();
            let fut = async move { rt_context.in_span(&span, || op.sink(morsel.as_data(), state)) };
            let result = compute_runtime.spawn(fut).await??;
            match result {
                BlockingSinkStatus::NeedMoreInput(new_state) => {
                    state = new_state;
                }
                BlockingSinkStatus::Finished(new_state) => {
                    return Ok(new_state);
                }
            }
        }

        Ok(state)
    }

    fn spawn_workers(
        op: Arc<dyn BlockingSink>,
        input_receivers: Vec<Receiver<PipelineResultType>>,
        task_set: &mut TaskSet<DaftResult<Box<dyn BlockingSinkState>>>,
        stats: Arc<RuntimeStatsContext>,
    ) {
        for input_receiver in input_receivers {
            task_set.spawn(Self::run_worker(op.clone(), input_receiver, stats.clone()));
        }
    }
}

impl TreeDisplay for BlockingSinkNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::Compact;
        if matches!(level, Compact) {
        } else {
            let rt_result = self.runtime_stats.result();
            rt_result.display(&mut display, true, true, true).unwrap();
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for BlockingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let child = self.child.as_mut();
        let child_results_receiver = child
            .start(false, runtime_handle)?
            .get_receiver_with_stats(&self.runtime_stats);

        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);
        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();
        let (input_senders, input_receivers) = (0..num_workers).map(|_| create_channel(1)).unzip();
        let dispatcher = op.make_dispatcher(runtime_handle);
        runtime_handle.spawn(
            async move {
                dispatcher
                    .dispatch(child_results_receiver, input_senders)
                    .await
            },
            self.name(),
        );

        runtime_handle.spawn(
            async move {
                let mut task_set = TaskSet::new();
                Self::spawn_workers(
                    op.clone(),
                    input_receivers,
                    &mut task_set,
                    runtime_stats.clone(),
                );

                let mut finished_states = Vec::with_capacity(num_workers);
                while let Some(result) = task_set.join_next().await {
                    let state = result.context(JoinSnafu)??;
                    finished_states.push(state);
                }

                let compute_runtime = get_compute_runtime();
                let finalized_result = compute_runtime
                    .spawn(async move {
                        runtime_stats.in_span(&info_span!("BlockingSinkNode::finalize"), || {
                            op.finalize(finished_states)
                        })
                    })
                    .await??;
                if let Some(res) = finalized_result {
                    let _ = destination_sender.send(res).await;
                }
                Ok(())
            },
            self.name(),
        );
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
