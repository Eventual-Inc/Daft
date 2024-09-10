use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::info_span;

use crate::{
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    pipeline::{PipelineNode, PipelineResultType},
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, NUM_CPUS,
};

pub trait BlockingSinkState: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    fn finalize(&mut self);
}

pub enum BlockingSinkStatus {
    NeedMoreInput,
    #[allow(dead_code)]
    Finished,
}

pub trait BlockingSink: Send + Sync {
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut dyn BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus>;
    fn finalize(&self, states: &[&dyn BlockingSinkState])
        -> DaftResult<Option<PipelineResultType>>;

    fn name(&self) -> &'static str;

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>>;

    fn max_parallelism(&self) -> usize {
        *NUM_CPUS
    }
}

pub(crate) struct BlockingSinkNode {
    op: Arc<dyn BlockingSink>,
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl BlockingSinkNode {
    pub(crate) fn new(op: Arc<dyn BlockingSink>, child: Box<dyn PipelineNode>) -> Self {
        let name = op.name();
        BlockingSinkNode {
            op,
            name,
            child,
            runtime_stats: RuntimeStatsContext::new(),
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    pub async fn run_sink_worker(
        op: Arc<dyn BlockingSink>,
        mut receiver: Receiver<PipelineResultType>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<Box<dyn BlockingSinkState>> {
        let mut state = op.make_state()?;
        let span = info_span!("BlockingSinkNode::Sink");
        while let Some(morsel) = receiver.recv().await {
            let morsel_data = morsel.as_data();
            let result = rt_context.in_span(&span, || op.sink(morsel_data, state.as_mut()))?;
            match result {
                BlockingSinkStatus::NeedMoreInput => {}
                BlockingSinkStatus::Finished => break,
            }
        }
        state.finalize();
        Ok(state)
    }

    pub fn spawn_workers(
        op: Arc<dyn BlockingSink>,
        receivers: Vec<Receiver<PipelineResultType>>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> tokio::task::JoinSet<DaftResult<Box<dyn BlockingSinkState>>> {
        let mut worker_set = tokio::task::JoinSet::new();
        for receiver in receivers {
            worker_set.spawn(Self::run_sink_worker(
                op.clone(),
                receiver,
                rt_context.clone(),
            ));
        }
        worker_set
    }

    pub async fn send_to_workers(
        mut receiver: CountingReceiver,
        worker_senders: Vec<Sender<PipelineResultType>>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |data: PipelineResultType| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send(data)
        };

        while let Some(morsel) = receiver.recv().await {
            let _ = send_to_next_worker(morsel).await;
        }
        Ok(())
    }
}

impl TreeDisplay for BlockingSinkNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::*;
        match level {
            Compact => {}
            _ => {
                let rt_result = self.runtime_stats.result();
                rt_result.display(&mut display, true, true, true).unwrap();
            }
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
        let rt_context = self.runtime_stats.clone();

        runtime_handle.spawn(
            async move {
                let (worker_senders, worker_receivers) = (0..op.max_parallelism())
                    .map(|_| create_channel::<PipelineResultType>(1))
                    .unzip();
                let mut worker_set =
                    Self::spawn_workers(op.clone(), worker_receivers, rt_context.clone());
                Self::send_to_workers(child_results_receiver, worker_senders).await?;

                let mut states = Vec::with_capacity(worker_set.len());
                while let Some(res) = worker_set.join_next().await {
                    let state = res.context(JoinSnafu)??;
                    states.push(state);
                }

                let finalized_result =
                    op.finalize(&states.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
                if let Some(part) = finalized_result {
                    let _ = destination_sender.send(part).await;
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
