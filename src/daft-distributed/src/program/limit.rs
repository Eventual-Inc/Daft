use std::collections::HashMap;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;

use super::{Program, RunningProgram};
use crate::{
    channel::{create_channel, Sender},
    scheduling::dispatcher::TaskDispatcherHandle,
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct LimitProgram {
    limit: usize,
    local_physical_plans: Vec<LocalPhysicalPlanRef>,
    children: Vec<Program>,
    input_psets: HashMap<String, Vec<PartitionRef>>,
}

impl LimitProgram {
    pub fn new(
        limit: usize,
        local_physical_plans: Vec<LocalPhysicalPlanRef>,
        children: Vec<Program>,
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
        _input_program: Option<RunningProgram>,
        _result_tx: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        todo!("Implement limit program loop");
    }

    pub fn run_program(mut self, stage_context: &mut StageContext) -> RunningProgram {
        let task_dispatcher_handle = stage_context.task_dispatcher_handle.clone();
        let input_program = if let Some(input_program) = self.children.pop() {
            assert!(self.children.is_empty());
            let input_running_program = input_program.run_program(stage_context);
            Some(input_running_program)
        } else {
            None
        };
        let (result_tx, result_rx) = create_channel(1);
        let program_loop = Self::program_loop(
            task_dispatcher_handle,
            self.local_physical_plans,
            input_program,
            result_tx,
        );
        stage_context.joinset.spawn(program_loop);

        RunningProgram::new(result_rx)
    }
}
