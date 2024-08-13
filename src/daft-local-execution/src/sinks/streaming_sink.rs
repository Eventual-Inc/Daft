use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    channel::{create_channel, MultiSender},
    pipeline::PipelineNode,
    NUM_CPUS,
};
use async_trait::async_trait;
pub enum StreamSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
    Finished(Option<Arc<MicroPartition>>),
}

pub trait StreamingSink: Send + Sync {
    fn execute(
        &mut self,
        index: usize,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<StreamSinkOutput>;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

pub(crate) struct StreamingSinkNode {
    // use a RW lock
    op: Arc<tokio::sync::Mutex<Box<dyn StreamingSink>>>,
    children: Vec<Box<dyn PipelineNode>>,
}

impl StreamingSinkNode {
    pub(crate) fn new(op: Box<dyn StreamingSink>, children: Vec<Box<dyn PipelineNode>>) -> Self {
        StreamingSinkNode {
            op: Arc::new(tokio::sync::Mutex::new(op)),
            children,
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for StreamingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    async fn start(&mut self, mut destination: MultiSender) -> DaftResult<()> {
        let (sender, mut streaming_receiver) = create_channel(*NUM_CPUS, destination.in_order());
        // now we can start building the right side
        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        child.start(sender).await?;
        let op = self.op.clone();
        tokio::spawn(async move {
            // this should be a RWLock and run in concurrent workers
            let span = info_span!("StreamingSink::execute");

            let mut sink = op.lock().await;
            let mut is_active = true;
            while is_active && let Some(val) = streaming_receiver.recv().await {
                let val = val?;
                loop {
                    let result = span.in_scope(|| sink.execute(0, &val))?;
                    match result {
                        StreamSinkOutput::HasMoreOutput(mp) => {
                            let sender = destination.get_next_sender();
                            sender.send(Ok(mp)).await.unwrap();
                        }
                        StreamSinkOutput::NeedMoreInput(mp) => {
                            if let Some(mp) = mp {
                                let sender = destination.get_next_sender();
                                sender.send(Ok(mp)).await.unwrap();
                            }
                            break;
                        }
                        StreamSinkOutput::Finished(mp) => {
                            if let Some(mp) = mp {
                                let sender = destination.get_next_sender();
                                sender.send(Ok(mp)).await.unwrap();
                            }
                            is_active = false;
                            break;
                        }
                    }
                }
            }
            DaftResult::Ok(())
        });
        Ok(())
    }
}
