use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    channel::{create_channel, MultiSender, PipelineOutput},
    pipeline::PipelineNode,
    ExecutionRuntimeHandle, NUM_CPUS,
};
use async_trait::async_trait;
pub enum BlockingSinkStatus {
    NeedMoreInput,
    #[allow(dead_code)]
    Finished,
}

pub trait BlockingSink: Send + Sync {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus>;
    fn finalize(&mut self) -> DaftResult<Option<PipelineOutput>>;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

pub(crate) struct BlockingSinkNode {
    // use a RW lock
    op: Arc<tokio::sync::Mutex<Box<dyn BlockingSink>>>,
    child: Box<dyn PipelineNode>,
}

impl BlockingSinkNode {
    pub(crate) fn new(op: Box<dyn BlockingSink>, child: Box<dyn PipelineNode>) -> Self {
        BlockingSinkNode {
            op: Arc::new(tokio::sync::Mutex::new(op)),
            child,
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for BlockingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    async fn start(
        &mut self,
        mut destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()> {
        let (sender, mut streaming_receiver) = create_channel(*NUM_CPUS, true);
        // now we can start building the right side
        let child = self.child.as_mut();
        child.start(sender, runtime_handle).await?;
        let op = self.op.clone();
        runtime_handle.spawn(async move {
            let span = info_span!("BlockingSinkNode::execute");
            let mut guard = op.lock().await;
            while let Some(val) = streaming_receiver.recv().await {
                if let BlockingSinkStatus::Finished =
                    span.in_scope(|| guard.sink(&val.as_micro_partition()?))?
                {
                    break;
                }
            }
            let finalized_result =
                info_span!("BlockingSinkNode::finalize").in_scope(|| guard.finalize())?;
            if let Some(part) = finalized_result {
                let _ = destination.get_next_sender().send(part).await;
            }
            Ok(())
        });
        Ok(())
    }
}
