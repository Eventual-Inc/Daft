use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_runtime, RuntimeRef};
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, create_ordering_aware_receiver_channel, Receiver, Sender},
    dispatcher::Dispatcher,
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, MaybeFuture, TaskSet, NUM_CPUS,
};

pub trait StreamingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub(crate) enum StreamingSinkOutputType {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
    Finished(Option<Arc<MicroPartition>>),
}

pub(crate) type StreamingSinkExecuteOutput =
    MaybeFuture<DaftResult<(Box<dyn StreamingSinkState>, StreamingSinkOutputType)>>;
pub(crate) type StreamingSinkFinalizeOutput = MaybeFuture<DaftResult<Option<Arc<MicroPartition>>>>;

pub trait StreamingSink: Send + Sync {
    /// Execute the StreamingSink operator on the morsel of input data,
    /// received from the child with the given index,
    /// with the given state.
    fn execute(
        &self,
        input: &Arc<MicroPartition>,
        state_handle: Box<dyn StreamingSinkState>,
        runtime: &RuntimeRef,
    ) -> StreamingSinkExecuteOutput;

    /// Finalize the StreamingSink operator, with the given states from each worker.
    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
        runtime: &RuntimeRef,
    ) -> StreamingSinkFinalizeOutput;

    /// The name of the StreamingSink operator.
    fn name(&self) -> &'static str;

    /// Create a new worker-local state for this StreamingSink.
    fn make_state(&self) -> Box<dyn StreamingSinkState>;

    /// The maximum number of concurrent workers that can be spawned for this sink.
    /// Each worker will has its own StreamingSinkState.
    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_dispatcher(
        &self,
        runtime_handle: &ExecutionRuntimeHandle,
        maintain_order: bool,
    ) -> Arc<dyn Dispatcher>;
}

pub struct StreamingSinkNode {
    op: Arc<dyn StreamingSink>,
    name: &'static str,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl StreamingSinkNode {
    pub(crate) fn new(op: Arc<dyn StreamingSink>, children: Vec<Box<dyn PipelineNode>>) -> Self {
        let name = op.name();
        Self {
            op,
            name,
            children,
            runtime_stats: RuntimeStatsContext::new(),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "StreamingSink::run_worker")]
    async fn run_worker(
        op: Arc<dyn StreamingSink>,
        input_receiver: Receiver<Arc<MicroPartition>>,
        output_sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<Box<dyn StreamingSinkState>> {
        let span = info_span!("StreamingSink::Execute");
        let compute_runtime = get_compute_runtime();
        let mut state = op.make_state();
        let mut finished = false;
        while let Some(morsel) = input_receiver.recv().await {
            if finished {
                break;
            }
            loop {
                let result = rt_context
                    .in_span(&span, || op.execute(&morsel, state, &compute_runtime))
                    .output()
                    .await??;
                state = result.0;
                match result.1 {
                    StreamingSinkOutputType::NeedMoreInput(mp) => {
                        if let Some(mp) = mp {
                            let _ = output_sender.send(mp).await;
                        }
                        break;
                    }
                    StreamingSinkOutputType::HasMoreOutput(mp) => {
                        let _ = output_sender.send(mp).await;
                    }
                    StreamingSinkOutputType::Finished(mp) => {
                        if let Some(mp) = mp {
                            let _ = output_sender.send(mp).await;
                        }
                        finished = true;
                        break;
                    }
                }
            }
        }

        Ok(state)
    }

    fn spawn_workers(
        op: Arc<dyn StreamingSink>,
        input_receivers: Vec<Receiver<Arc<MicroPartition>>>,
        output_senders: Vec<Sender<Arc<MicroPartition>>>,
        task_set: &mut TaskSet<DaftResult<Box<dyn StreamingSinkState>>>,
        stats: Arc<RuntimeStatsContext>,
    ) {
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_senders) {
            task_set.spawn(Self::run_worker(
                op.clone(),
                input_receiver,
                output_sender,
                stats.clone(),
            ));
        }
    }
}

impl TreeDisplay for StreamingSinkNode {
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
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl PipelineNode for StreamingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let child_result_channel = child.start(maintain_order, runtime_handle)?;
            let counting_receiver =
                CountingReceiver::new(child_result_channel, self.runtime_stats.clone());
            child_result_receivers.push(counting_receiver);
        }

        let (destination_sender, destination_receiver) = create_channel(1);
        let destination_sender =
            CountingSender::new(destination_sender, self.runtime_stats.clone());

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();

        let dispatcher = op.make_dispatcher(runtime_handle, maintain_order);
        let input_receivers = dispatcher.dispatch(
            child_result_receivers,
            num_workers,
            runtime_handle,
            self.name(),
        );
        runtime_handle.spawn(
            async move {
                let mut task_set = TaskSet::new();
                let (output_senders, mut output_receiver) =
                    create_ordering_aware_receiver_channel(maintain_order, num_workers);
                Self::spawn_workers(
                    op.clone(),
                    input_receivers,
                    output_senders,
                    &mut task_set,
                    runtime_stats.clone(),
                );

                while let Some(morsel) = output_receiver.recv().await {
                    let _ = destination_sender.send(morsel).await;
                }

                let mut finished_states = Vec::with_capacity(num_workers);
                while let Some(result) = task_set.join_next().await {
                    let state = result.context(JoinSnafu)??;
                    finished_states.push(state);
                }

                let compute_runtime = get_compute_runtime();
                let finalized_result = runtime_stats
                    .in_span(&info_span!("StreamingSinkNode::finalize"), || {
                        op.finalize(finished_states, &compute_runtime)
                    })
                    .output()
                    .await??;
                if let Some(res) = finalized_result {
                    let _ = destination_sender.send(res).await;
                }
                Ok(())
            },
            self.name(),
        );
        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
