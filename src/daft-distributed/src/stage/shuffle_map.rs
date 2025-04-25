use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_logical_plan::LogicalPlanRef;

use crate::{
    channel::Receiver, program::logical_plan_to_programs, runtime::JoinSet,
    scheduling::dispatcher::TaskDispatcherHandle,
};

pub struct ShuffleMapStage {
    logical_plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
}

impl ShuffleMapStage {
    #[allow(dead_code)]
    pub fn new(logical_plan: LogicalPlanRef, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            logical_plan,
            config,
        }
    }
}

impl ShuffleMapStage {
    pub fn spawn_stage_programs(
        &self,
        mut psets: HashMap<String, Vec<PartitionRef>>,
        task_dispatcher_handle: TaskDispatcherHandle,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> DaftResult<Receiver<PartitionRef>> {
        let programs = logical_plan_to_programs(self.logical_plan.clone())?;
        let config = self.config.clone();
        let mut next_receiver = None;
        for program in programs {
            next_receiver = Some(program.spawn_program(
                task_dispatcher_handle.clone(),
                config.clone(),
                std::mem::take(&mut psets),
                joinset,
                next_receiver.take(),
            ));
        }
        Ok(next_receiver.unwrap())
    }
}
