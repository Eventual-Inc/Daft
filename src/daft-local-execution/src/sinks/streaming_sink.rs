use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::info_span;

use crate::{
    channel::PipelineChannel, pipeline::PipelineNode, runtime_stats::RuntimeStatsContext,
    ExecutionRuntimeHandle, NUM_CPUS,
};

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

pub struct StreamingSinkNode {
    // use a RW lock
    op: Arc<tokio::sync::Mutex<Box<dyn StreamingSink>>>,
    name: &'static str,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl StreamingSinkNode {
    pub(crate) fn new(op: Box<dyn StreamingSink>, children: Vec<Box<dyn PipelineNode>>) -> Self {
        let name = op.name();
        Self {
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
        use common_display::DisplayLevel::Compact;
        if matches!(level, Compact) {
        } else {
            let rt_result = self.runtime_stats.result();
            rt_result.display(&mut display, true, true, true).unwrap();
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

impl PipelineNode for StreamingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        let child_results_channel = child.start(true, runtime_handle)?;
        let mut child_results_receiver =
            child_results_channel.get_receiver_with_stats(&self.runtime_stats);

        let mut destination_channel = PipelineChannel::new(*NUM_CPUS, maintain_order);
        let sender = destination_channel.get_next_sender_with_stats(&self.runtime_stats);
        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                // this should be a RWLock and run in concurrent workers
                let span = info_span!("StreamingSink::execute");

                let mut sink = op.lock().await;
                let mut is_active = true;
                while is_active && let Some(val) = child_results_receiver.recv().await {
                    let val = val.as_data();
                    loop {
                        let result = runtime_stats.in_span(&span, || sink.execute(0, val))?;
                        match result {
                            StreamSinkOutput::HasMoreOutput(mp) => {
                                sender.send(mp.into()).await.unwrap();
                            }
                            StreamSinkOutput::NeedMoreInput(mp) => {
                                if let Some(mp) = mp {
                                    sender.send(mp.into()).await.unwrap();
                                }
                                break;
                            }
                            StreamSinkOutput::Finished(mp) => {
                                if let Some(mp) = mp {
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
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
