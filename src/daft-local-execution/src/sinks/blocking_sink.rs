use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    channel::{create_channel, MultiSender},
    pipeline::PipelineNode,
    runtime_stats::RuntimeStatsContext,
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
    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>>;
    fn name(&self) -> &'static str;
}

pub(crate) struct BlockingSinkNode {
    // use a RW lock
    op: Arc<tokio::sync::Mutex<Box<dyn BlockingSink>>>,
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl BlockingSinkNode {
    pub(crate) fn new(op: Box<dyn BlockingSink>, child: Box<dyn PipelineNode>) -> Self {
        let name = op.name();
        BlockingSinkNode {
            op: Arc::new(tokio::sync::Mutex::new(op)),
            name,
            child,
            runtime_stats: Arc::new(RuntimeStatsContext::new(name.to_string())),
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

    fn name(&self) -> &'static str {
        self.name
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

        let rt_context = self.runtime_stats.clone();
        runtime_handle.spawn(async move {
            let span = info_span!("BlockingSinkNode::execute");
            let mut guard = op.lock().await;
            while let Some(val) = streaming_receiver.recv().await {
                rt_context.mark_rows_received(val.len() as u64);
                if let BlockingSinkStatus::Finished =
                    rt_context.in_span(&span, || guard.sink(&val))?
                {
                    break;
                }
            }
            let finalized_result = rt_context
                .in_span(&info_span!("BlockingSinkNode::finalize"), || {
                    guard.finalize()
                })?;
            if let Some(part) = finalized_result {
                let len = part.len();
                let _ = destination.get_next_sender().send(part).await;
                rt_context.mark_rows_emitted(len as u64);
            }
            Ok(())
        });
        Ok(())
    }
}
