use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    channel::create_multi_channel,
    pipeline::{PipelineNode, PipelineResultReceiver},
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
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineResultReceiver> {
        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        let mut child_results_receiver = child.start(true, runtime_handle).await?;
        let (mut destination_sender, destination_receiver) =
            create_multi_channel(*NUM_CPUS, maintain_order);
        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                // this should be a RWLock and run in concurrent workers
                let span = info_span!("StreamingSink::execute");

                let mut sink = op.lock().await;
                let mut is_active = true;
                while is_active && let Some(val) = child_results_receiver.recv().await {
                    let val = val?;
                    let val = val.as_data();
                    runtime_stats.mark_rows_received(val.len() as u64);
                    loop {
                        let result = runtime_stats.in_span(&span, || sink.execute(0, val))?;
                        match result {
                            StreamSinkOutput::HasMoreOutput(mp) => {
                                let sender = destination_sender.get_next_sender(&runtime_stats);
                                sender.send(mp.into()).await.unwrap();
                            }
                            StreamSinkOutput::NeedMoreInput(mp) => {
                                if let Some(mp) = mp {
                                    let sender = destination_sender.get_next_sender(&runtime_stats);
                                    sender.send(mp.into()).await.unwrap();
                                }
                                break;
                            }
                            StreamSinkOutput::Finished(mp) => {
                                if let Some(mp) = mp {
                                    let sender = destination_sender.get_next_sender(&runtime_stats);
                                    sender.send(mp.into()).await.unwrap();
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
        Ok(destination_receiver.into())
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
