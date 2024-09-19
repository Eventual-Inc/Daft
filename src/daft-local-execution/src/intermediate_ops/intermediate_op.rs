use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use super::buffer::OperatorBuffer;
use crate::{
    channel::{create_channel, create_ordering_aware_channel, PipelineChannel, Receiver, Sender},
    create_worker_set,
    pipeline::{PipelineNode, PipelineResultType},
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, WorkerSet, NUM_CPUS,
};

pub trait IntermediateOperatorState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
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
        state: Option<&mut Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<IntermediateOperatorResult>;
    fn name(&self) -> &'static str;
    fn make_state(&self) -> Option<Box<dyn IntermediateOperatorState>> {
        None
    }
}

pub(crate) struct IntermediateNode {
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
    async fn run_worker(
        op: Arc<dyn IntermediateOperator>,
        mut input_receiver: Receiver<(usize, PipelineResultType)>,
        output_sender: Sender<Arc<MicroPartition>>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<()> {
        let span = info_span!("IntermediateOp::execute");
        let mut state = op.make_state();
        while let Some((idx, morsel)) = input_receiver.recv().await {
            loop {
                let result =
                    rt_context.in_span(&span, || op.execute(idx, &morsel, state.as_mut()))?;
                match result {
                    IntermediateOperatorResult::NeedMoreInput(Some(mp)) => {
                        let _ = output_sender.send(mp).await;
                        break;
                    }
                    IntermediateOperatorResult::NeedMoreInput(None) => {
                        break;
                    }
                    IntermediateOperatorResult::HasMoreOutput(mp) => {
                        let _ = output_sender.send(mp).await;
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_workers(
        op: Arc<dyn IntermediateOperator>,
        input_receivers: Vec<Receiver<(usize, PipelineResultType)>>,
        output_senders: Vec<Sender<Arc<MicroPartition>>>,
        worker_set: &mut WorkerSet<DaftResult<()>>,
        stats: Arc<RuntimeStatsContext>,
    ) {
        for (input_receiver, output_sender) in input_receivers.into_iter().zip(output_senders) {
            worker_set.spawn(Self::run_worker(
                op.clone(),
                input_receiver,
                output_sender,
                stats.clone(),
            ));
        }
    }

    async fn forward_input_to_workers(
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
            let mut buffer = OperatorBuffer::new(morsel_size);
            while let Some(morsel) = receiver.recv().await {
                if morsel.should_broadcast() {
                    for worker_sender in worker_senders.iter() {
                        let _ = worker_sender.send((idx, morsel.clone())).await;
                    }
                } else {
                    buffer.push(morsel.as_data().clone());
                    if let Some(ready) = buffer.try_clear() {
                        let _ = send_to_next_worker(idx, ready?.into()).await;
                    }
                }
            }
            // Buffer may still have some morsels left above the threshold
            while let Some(ready) = buffer.try_clear() {
                let _ = send_to_next_worker(idx, ready?.into()).await;
            }
            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.clear_all() {
                let _ = send_to_next_worker(idx, last_morsel?.into()).await;
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
        self.children.iter().map(|v| v.as_tree_display()).collect()
    }
}

impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
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
        for child in self.children.iter_mut() {
            let child_result_channel = child.start(maintain_order, runtime_handle)?;
            child_result_receivers
                .push(child_result_channel.get_receiver_with_stats(&self.runtime_stats));
        }

        let destination_channel = PipelineChannel::new();
        let destination_sender = destination_channel.get_sender_with_stats(&self.runtime_stats);

        let op = self.intermediate_op.clone();
        let stats = self.runtime_stats.clone();
        let morsel_size = runtime_handle.default_morsel_size();
        runtime_handle.spawn(
            async move {
                let num_workers = *NUM_CPUS;
                let (input_senders, input_receivers) =
                    (0..num_workers).map(|_| create_channel(1)).unzip();
                let (output_senders, mut output_receiver) =
                    create_ordering_aware_channel(num_workers, maintain_order);

                let mut worker_set = create_worker_set();
                Self::spawn_workers(
                    op.clone(),
                    input_receivers,
                    output_senders,
                    &mut worker_set,
                    stats.clone(),
                );

                Self::forward_input_to_workers(child_result_receivers, input_senders, morsel_size)
                    .await?;

                while let Some(morsel) = output_receiver.recv().await {
                    let _ = destination_sender.send(morsel.into()).await;
                }
                while let Some(result) = worker_set.join_next().await {
                    result.context(JoinSnafu)??;
                }
                Ok(())
            },
            self.intermediate_op.name(),
        );
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
