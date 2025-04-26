use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;

use super::{ProgramContext, RunningProgram};
use crate::{
    channel::{create_channel, Sender},
    scheduling::dispatcher::TaskDispatcherHandle,
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct LimitProgram {
    limit: usize,
    program_context: ProgramContext,
}

impl LimitProgram {
    pub fn new(limit: usize, program_context: ProgramContext) -> Self {
        Self {
            limit,
            program_context,
        }
    }

    async fn program_loop(
        _task_dispatcher_handle: TaskDispatcherHandle,
        _local_physical_plans: Vec<LocalPhysicalPlanRef>,
        _input_program: Option<RunningProgram>,
        _result_tx: Sender<PartitionRef>,
    ) -> DaftResult<()> {
        todo!()
    }

    pub fn run_program(mut self, stage_context: &mut StageContext) -> RunningProgram {
        let task_dispatcher_handle = stage_context.task_dispatcher_handle.clone();
        let input_program = if let Some(input_program) = self.program_context.input_programs.pop() {
            assert!(self.program_context.input_programs.is_empty());
            let input_running_program = input_program.run_program(stage_context);
            Some(input_running_program)
        } else {
            None
        };
        let (result_tx, result_rx) = create_channel(1);
        let program_loop = Self::program_loop(
            task_dispatcher_handle,
            self.program_context.local_physical_plans,
            input_program,
            result_tx,
        );
        stage_context.joinset.spawn(program_loop);

        RunningProgram::new(result_rx)
    }
}
