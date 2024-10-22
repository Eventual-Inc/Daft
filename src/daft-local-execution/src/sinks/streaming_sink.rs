use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    create_task_set,
    pipeline::{PipelineNode, PipelineResultType},
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, TaskSet, NUM_CPUS,
};

pub trait StreamingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub enum StreamingSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
    Finished(Option<Arc<MicroPartition>>),
}

pub trait StreamingSink: Send + Sync {
    /// Execute the StreamingSink operator on the morsel of input data,
    /// received from the child with the given index,
    /// with the given state.
    fn execute(
        &self,
        index: usize,
        input: &PipelineResultType,
        state: &mut dyn StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput>;

    /// Finalize the StreamingSink operator, with the given states from each worker.
    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>>;

    /// The name of the StreamingSink operator.
    fn name(&self) -> &'static str;

    /// Create a new worker-local state for this StreamingSink.
    fn make_state(&self) -> Box<dyn StreamingSinkState>;

    /// The maximum number of concurrent workers that can be spawned for this sink.
    /// Each worker will has its own StreamingSinkState.
    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }
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
        mut input_receiver: Receiver<(usize, PipelineResultType)>,
        output_sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<Box<dyn StreamingSinkState>> {
        let span = info_span!("StreamingSink::Execute");
        let mut state = op.make_state();
        while let Some((idx, morsel)) = input_receiver.recv().await {
            loop {
                let result =
                    rt_context.in_span(&span, || op.execute(idx, &morsel, state.as_mut()))?;
                match result {
                    StreamingSinkOutput::NeedMoreInput(mp) => {
                        if let Some(mp) = mp {
                            let _ = output_sender.send(mp).await;
                        }
                        break;
                    }
                    StreamingSinkOutput::HasMoreOutput(mp) => {
                        let _ = output_sender.send(mp).await;
                    }
                    StreamingSinkOutput::Finished(mp) => {
                        if let Some(mp) = mp {
                            let _ = output_sender.send(mp).await;
                        }
                        return Ok(state);
                    }
                }
            }
        }
        Ok(state)
    }

    fn spawn_workers(
        op: Arc<dyn StreamingSink>,
        input_receivers: Vec<Receiver<(usize, PipelineResultType)>>,
        task_set: &mut TaskSet<DaftResult<Box<dyn StreamingSinkState>>>,
        stats: Arc<RuntimeStatsContext>,
    ) -> Receiver<Arc<MicroPartition>> {
        let (output_sender, output_receiver) = create_channel(input_receivers.len());
        for input_receiver in input_receivers {
            task_set.spawn(Self::run_worker(
                op.clone(),
                input_receiver,
                output_sender.clone(),
                stats.clone(),
            ));
        }
        output_receiver
    }

    // Forwards input from the children to the workers in a round-robin fashion.
    // Always exhausts the input from one child before moving to the next.
    async fn forward_input_to_workers(
        receivers: Vec<CountingReceiver>,
        worker_senders: Vec<Sender<(usize, PipelineResultType)>>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |idx, data: PipelineResultType| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send((idx, data))
        };

        for (idx, mut receiver) in receivers.into_iter().enumerate() {
            while let Some(morsel) = receiver.recv().await {
                if morsel.should_broadcast() {
                    for worker_sender in &worker_senders {
                        let _ = worker_sender.send((idx, morsel.clone())).await;
                    }
                } else {
                    let _ = send_to_next_worker(idx, morsel.clone()).await;
                }
            }
        }
        Ok(())
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
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let child_result_channel = child.start(maintain_order, runtime_handle)?;
            child_result_receivers
                .push(child_result_channel.get_receiver_with_stats(&self.runtime_stats.clone()));
        }

        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);

        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();
        let (input_senders, input_receivers) = (0..num_workers).map(|_| create_channel(1)).unzip();
        runtime_handle.spawn(
            Self::forward_input_to_workers(child_result_receivers, input_senders),
            self.name(),
        );
        runtime_handle.spawn(
            async move {
                let mut task_set = create_task_set();
                let mut output_receiver = Self::spawn_workers(
                    op.clone(),
                    input_receivers,
                    &mut task_set,
                    runtime_stats.clone(),
                );

                while let Some(morsel) = output_receiver.recv().await {
                    let _ = destination_sender.send(morsel.into()).await;
                }

                let mut finished_states = Vec::with_capacity(num_workers);
                while let Some(result) = task_set.join_next().await {
                    let state = result.context(JoinSnafu)??;
                    finished_states.push(state);
                }

                if let Some(finalized_result) = op.finalize(finished_states)? {
                    let _ = destination_sender.send(finalized_result.into()).await;
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
