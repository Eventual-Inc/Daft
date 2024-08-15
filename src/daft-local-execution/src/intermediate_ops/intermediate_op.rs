use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tokio::sync::mpsc::error::SendError;
use tracing::{info_span, instrument};

use async_trait::async_trait;

use crate::{
    channel::{
        create_channel, create_single_channel, MultiReceiver, MultiSender, SingleReceiver,
        SingleSender,
    },
    pipeline::PipelineNode,
    runtime_stats::{CountingSender, RuntimeStatsContext},
    ExecutionRuntimeHandle, NUM_CPUS,
};

use super::state::OperatorTaskState;
pub trait IntermediateOperator: Send + Sync {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
    fn name(&self) -> &'static str;
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
        let rts: RuntimeStatsContext = RuntimeStatsContext::new(intermediate_op.name().to_string());
        Self::new_with_runtime_stats(intermediate_op, children, Arc::new(rts))
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
        mut receiver: SingleReceiver,
        sender: SingleSender,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<()> {
        let mut state = OperatorTaskState::new();
        let span = info_span!("IntermediateOp::execute");
        let sender = CountingSender::new(sender, rt_context.clone());
        while let Some(morsel) = receiver.recv().await {
            rt_context.mark_rows_received(morsel.len() as u64);
            let result = rt_context.in_span(&span, || op.execute(&morsel))?;
            state.add(result);
            if let Some(part) = state.try_clear() {
                let _ = sender.send(part?).await;
            }
        }
        if let Some(part) = state.clear() {
            let _ = sender.send(part?).await;
        }
        Ok(())
    }

    pub async fn spawn_workers(
        &self,
        destination: &mut MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> Vec<SingleSender> {
        let num_senders = destination.buffer_size();
        let mut worker_senders = Vec::with_capacity(num_senders);
        for _ in 0..num_senders {
            let (worker_sender, worker_receiver) = create_single_channel(1);
            let destination_sender = destination.get_next_sender();
            runtime_handle.spawn(Self::run_worker(
                self.intermediate_op.clone(),
                worker_receiver,
                destination_sender,
                self.runtime_stats.clone(),
            ));
            worker_senders.push(worker_sender);
        }
        worker_senders
    }

    pub async fn send_to_workers(
        mut receiver: MultiReceiver,
        worker_senders: Vec<SingleSender>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        while let Some(morsel) = receiver.recv().await {
            if morsel.is_empty() {
                continue;
            }

            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            let _ = next_worker_sender.send(morsel).await;
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
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
                writeln!(display).unwrap();
                writeln!(display, "rows received = {}", rt_result.rows_received).unwrap();
                writeln!(display, "rows emitted = {}", rt_result.rows_emitted).unwrap();
                writeln!(
                    display,
                    "CPU-ms = {:.1}",
                    (rt_result.cpu_us as f64) / 1000f64
                )
                .unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children.iter().map(|v| v.as_tree_display()).collect()
    }
}

#[async_trait]
impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    fn name(&self) -> &'static str {
        self.intermediate_op.name()
    }

    async fn start(
        &mut self,
        mut destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()> {
        assert_eq!(
            self.children.len(),
            1,
            "we only support 1 child for Intermediate Node for now"
        );
        let (sender, receiver) = create_channel(*NUM_CPUS, destination.in_order());
        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        child.start(sender, runtime_handle).await?;

        let worker_senders = self.spawn_workers(&mut destination, runtime_handle).await;
        runtime_handle.spawn(Self::send_to_workers(receiver, worker_senders));
        Ok(())
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
