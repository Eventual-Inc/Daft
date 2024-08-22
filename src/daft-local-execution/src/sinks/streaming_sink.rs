use std::sync::Arc;

use common_display::tree::TreeDisplay;
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
    name: &'static str,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl StreamingSinkNode {
    pub(crate) fn new(op: Box<dyn StreamingSink>, children: Vec<Box<dyn PipelineNode>>) -> Self {
        let name = op.name();
        StreamingSinkNode {
            op: Arc::new(tokio::sync::Mutex::new(op)),
            name,
            children,
            runtime_stats: RuntimeStatsContext::new(),
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for StreamingSinkNode {
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
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

#[async_trait]
impl PipelineNode for StreamingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    fn name(&self) -> &'static str {
        self.name
    }

    async fn start(
        &mut self,
        mut destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<()> {
        let (sender, mut streaming_receiver) = create_channel(*NUM_CPUS, destination.in_order());
        // now we can start building the right side
        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        child.start(sender, runtime_handle).await?;
        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                // this should be a RWLock and run in concurrent workers
                let span = info_span!("StreamingSink::execute");

                let mut sink = op.lock().await;
                let mut is_active = true;
                while is_active && let Some(val) = streaming_receiver.recv().await {
                    runtime_stats.mark_rows_received(val.len() as u64);
                    loop {
                        let result = runtime_stats.in_span(&span, || sink.execute(0, &val))?;
                        match result {
                            StreamSinkOutput::HasMoreOutput(mp) => {
                                let len = mp.len() as u64;
                                let sender = destination.get_next_sender();
                                sender.send(mp).await.unwrap();
                                runtime_stats.mark_rows_emitted(len);
                            }
                            StreamSinkOutput::NeedMoreInput(mp) => {
                                if let Some(mp) = mp {
                                    let len = mp.len() as u64;
                                    let sender = destination.get_next_sender();
                                    sender.send(mp).await.unwrap();
                                    runtime_stats.mark_rows_emitted(len);
                                }
                                break;
                            }
                            StreamSinkOutput::Finished(mp) => {
                                if let Some(mp) = mp {
                                    let len = mp.len() as u64;
                                    let sender = destination.get_next_sender();
                                    sender.send(mp).await.unwrap();
                                    runtime_stats.mark_rows_emitted(len);
                                }
                                is_active = false;
                                break;
                            }
                        }
                    }
                }
                DaftResult::Ok(())
            },
            self.name(),
        );
        Ok(())
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
