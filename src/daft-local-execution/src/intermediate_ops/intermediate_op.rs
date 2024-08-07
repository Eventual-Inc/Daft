use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use async_trait::async_trait;

use crate::{
    channel::{
        create_channel, create_single_channel, MultiReceiver, MultiSender, SingleReceiver,
        SingleSender,
    },
    pipeline::PipelineNode,
    WorkerSet, NUM_CPUS,
};

pub trait IntermediateOpSpec: Send + Sync {
    fn to_operator(&self) -> Box<dyn IntermediateOperator>;
}

pub enum OperatorOutput {
    Ready(Arc<MicroPartition>),
    NeedMoreInput,
}

pub trait IntermediateOperator: Send + Sync {
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<OperatorOutput>;
    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>>;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

pub(crate) struct IntermediateNode {
    intermediate_op_spec: Arc<dyn IntermediateOpSpec>,
    children: Vec<Box<dyn PipelineNode>>,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op_spec: Arc<dyn IntermediateOpSpec>,
        children: Vec<Box<dyn PipelineNode>>,
    ) -> Self {
        IntermediateNode {
            intermediate_op_spec,
            children,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "IntermediateOperator::run_worker")]
    pub async fn run_worker(
        spec: Arc<dyn IntermediateOpSpec>,
        mut receiver: SingleReceiver,
        sender: SingleSender,
    ) -> DaftResult<()> {
        let mut operator = spec.to_operator();
        let span = info_span!("IntermediateOp::execute");
        while let Some(morsel) = receiver.recv().await {
            let result = span.in_scope(|| operator.execute(&morsel))?;
            match result {
                OperatorOutput::Ready(part) => {
                    let _ = sender.send(part).await;
                }
                OperatorOutput::NeedMoreInput => {}
            }
        }
        if let Some(part) = operator.finalize()? {
            let _ = sender.send(part).await;
        }
        Ok(())
    }

    pub fn spawn_workers(
        &self,
        worker_set: &mut WorkerSet,
        destination: &mut MultiSender,
    ) -> Vec<SingleSender> {
        let num_senders = destination.buffer_size();
        let mut worker_senders = Vec::with_capacity(num_senders);
        for _ in 0..num_senders {
            let (worker_sender, worker_receiver) = create_single_channel(1);
            let destination_sender = destination.get_next_sender();
            worker_set.spawn(Self::run_worker(
                self.intermediate_op_spec.clone(),
                worker_receiver,
                destination_sender,
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

#[async_trait]
impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    async fn start(
        &mut self,
        mut destination: MultiSender,
        worker_set: &mut WorkerSet,
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
        child.start(sender, worker_set).await?;

        let worker_senders = self.spawn_workers(worker_set, &mut destination);
        worker_set.spawn(Self::send_to_workers(receiver, worker_senders));
        Ok(())
    }
}
