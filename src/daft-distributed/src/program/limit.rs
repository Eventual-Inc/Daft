use std::collections::HashMap;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    channel::{create_channel, Sender},
    scheduling::dispatcher::TaskDispatcherHandle,
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct LimitProgram {
    limit: usize,
    local_physical_plans: Vec<LocalPhysicalPlanRef>,
    children: Vec<Box<dyn DistributedPipelineNode>>,
    input_psets: HashMap<String, Vec<PartitionRef>>,
}

impl LimitProgram {
    pub fn new(
        limit: usize,
        local_physical_plans: Vec<LocalPhysicalPlanRef>,
        children: Vec<Box<dyn DistributedPipelineNode>>,
        input_psets: HashMap<String, Vec<PartitionRef>>,
    ) -> Self {
        // We cannot have empty local physical plans
        assert!(!local_physical_plans.is_empty());
        // If we have children, we must have input psets, and we must have a single local physical plan
        if !children.is_empty() {
            assert!(input_psets.is_empty());
            assert!(local_physical_plans.len() == 1);
        }
        Self {
            limit,
            local_physical_plans,
            children,
            input_psets,
        }
    }

    async fn program_loop(
        _task_dispatcher_handle: TaskDispatcherHandle,
        _local_physical_plans: Vec<LocalPhysicalPlanRef>,
        _input_node: Option<RunningPipelineNode>,
        _input_psets: HashMap<String, Vec<PartitionRef>>,
        _result_tx: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        todo!("Implement limit program loop");
    }
}

impl DistributedPipelineNode for LimitProgram {
    fn name(&self) -> &'static str {
        "Limit"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        self.children.iter().map(|c| c.as_ref()).collect()
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let task_dispatcher_handle = stage_context.task_dispatcher_handle.clone();
        let input_program = if let Some(mut input_program) = self.children.pop() {
            assert!(self.children.is_empty());
            let input_running_program = input_program.start(stage_context);
            Some(input_running_program)
        } else {
            None
        };
        let (result_tx, result_rx) = create_channel(1);
        let program_loop = Self::program_loop(
            task_dispatcher_handle,
            std::mem::take(&mut self.local_physical_plans),
            input_program,
            std::mem::take(&mut self.input_psets),
            result_tx,
        );
        stage_context.joinset.spawn(program_loop);

        RunningPipelineNode::new(result_rx)
    }
}
