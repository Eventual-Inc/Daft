use common_error::DaftResult;

use super::{
    translate::PipelinePlan, DistributedPipelineNode, PipelineOutput, RunningPipelineNode,
};
use crate::{
    channel::{create_channel, Sender},
    scheduling::dispatcher::TaskDispatcherHandle,
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct CollectNode {
    plan: PipelinePlan,
    children: Vec<Box<dyn DistributedPipelineNode>>,
}

impl CollectNode {
    #[allow(dead_code)]
    pub fn new(plan: PipelinePlan, children: Vec<Box<dyn DistributedPipelineNode>>) -> Self {
        Self { plan, children }
    }

    #[allow(dead_code)]
    async fn execution_loop(
        _task_dispatcher_handle: TaskDispatcherHandle,
        _plan: PipelinePlan,
        _input_node: Option<RunningPipelineNode>,
        _result_tx: Sender<PipelineOutput>,
    ) -> DaftResult<()> {
        todo!("FLOTILLA_MS1: Implement collect execution sloop");
    }
}

impl DistributedPipelineNode for CollectNode {
    fn name(&self) -> &'static str {
        "Collect"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        self.children.iter().map(|c| c.as_ref()).collect()
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
        let input_node = if let Some(mut input_node) = self.children.pop() {
            assert!(self.children.is_empty());
            let input_running_node = input_node.start(stage_context);
            Some(input_running_node)
        } else {
            None
        };
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            task_dispatcher_handle,
            self.plan.clone(),
            input_node,
            result_tx,
        );
        stage_context.spawn_task_on_joinset(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}
