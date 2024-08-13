use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use async_trait::async_trait;

use crate::{
    channel::{
        create_channel, create_single_channel, MultiReceiver, MultiSender, PipelineOutput,
        SingleReceiver, SingleSender,
    },
    pipeline::PipelineNode,
    ExecutionRuntimeHandle, NUM_CPUS,
};

use super::state::{GenericOperatorState, OperatorState};

pub enum Ordering {
    MaintainParentOrder,
}

pub enum IntermediateOperatorOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
}

pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        index: usize,
        input: &PipelineOutput,
        state: &mut Box<dyn OperatorState>,
    ) -> DaftResult<IntermediateOperatorOutput>;
    fn finalize(&self, state: &mut Box<dyn OperatorState>) -> DaftResult<Option<PipelineOutput>>;
    fn make_state(&self) -> Box<dyn OperatorState> {
        Box::new(GenericOperatorState::default())
    }
    fn required_ordering(&self) -> Ordering {
        Ordering::MaintainParentOrder
    }
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

pub(crate) struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
    ) -> Self {
        IntermediateNode {
            intermediate_op,
            children,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "IntermediateOperator::run_worker")]
    pub async fn run_worker(
        op: Arc<dyn IntermediateOperator>,
        mut receiver: SingleReceiver<(usize, PipelineOutput)>,
        sender: SingleSender<PipelineOutput>,
    ) -> DaftResult<()> {
        let mut state = op.make_state();
        let span = info_span!("IntermediateOp::execute");
        while let Some((idx, morsel)) = receiver.recv().await {
            let result = span.in_scope(|| op.execute(idx, &morsel, &mut state))?;
            match result {
                IntermediateOperatorOutput::NeedMoreInput(part) => {
                    if let Some(part) = part {
                        let _ = sender.send(part.into()).await;
                    }
                }
            }
        }
        if let Some(part) = op.finalize(&mut state)? {
            let _ = sender.send(part).await;
        }
        Ok(())
    }

    pub async fn spawn_workers(
        &self,
        destination: &mut MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> Vec<SingleSender<(usize, PipelineOutput)>> {
        let num_senders = destination.buffer_size();
        let mut worker_senders = Vec::with_capacity(num_senders);
        for _ in 0..num_senders {
            let (worker_sender, worker_receiver) = create_single_channel(1);
            let destination_sender = destination.get_next_sender();
            runtime_handle.spawn(Self::run_worker(
                self.intermediate_op.clone(),
                worker_receiver,
                destination_sender,
            ));
            worker_senders.push(worker_sender);
        }
        worker_senders
    }

    pub async fn send_to_workers(
        child_receivers: Vec<MultiReceiver>,
        worker_senders: Vec<SingleSender<(usize, PipelineOutput)>>,
    ) -> DaftResult<()> {
        for (idx, mut receiver) in child_receivers.into_iter().enumerate() {
            let mut next_worker_idx = 0;
            while let Some(morsel) = receiver.recv().await {
                if morsel.should_broadcast() {
                    for sender in worker_senders.iter() {
                        let _ = sender.send((idx, morsel.clone())).await;
                    }
                } else {
                    let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
                    let _ = next_worker_sender.send((idx, morsel)).await;
                    next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    async fn start(
        &mut self,
        mut destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()> {
        let child_order = match self.intermediate_op.required_ordering() {
            Ordering::MaintainParentOrder => destination.in_order(),
        };
        let mut child_receivers = vec![];
        for child in self.children.iter_mut() {
            let (sender, receiver) = create_channel(*NUM_CPUS, child_order);
            child.start(sender, runtime_handle).await?;
            child_receivers.push(receiver);
        }

        let worker_senders = self.spawn_workers(&mut destination, runtime_handle).await;
        runtime_handle.spawn(Self::send_to_workers(child_receivers, worker_senders));
        Ok(())
    }
}
