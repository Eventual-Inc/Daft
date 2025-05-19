use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_logical_plan::LogicalPlanBuilder;

use crate::{
    stage::StagePlan,
    utils::{
        channel::{Receiver, ReceiverStream},
        joinset::JoinSet,
        stream::JoinableForwardingStream,
    },
};

mod runner;
pub(crate) use runner::PlanRunner;

pub(crate) struct DistributedPhysicalPlan {
    stage_plan: StagePlan,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let logical_plan = builder.build();
        let stage_plan = StagePlan::from_logical_plan(logical_plan)?;

        Ok(Self { stage_plan, config })
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }

    pub fn stage_plan(&self) -> &StagePlan {
        &self.stage_plan
    }
}

pub(crate) type PlanResultStream = JoinableForwardingStream<ReceiverStream<PartitionRef>>;

pub(crate) struct PlanResult {
    joinset: JoinSet<DaftResult<()>>,
    rx: Receiver<PartitionRef>,
}

impl PlanResult {
    fn new(joinset: JoinSet<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self { joinset, rx }
    }

    pub fn into_stream(self) -> PlanResultStream {
        JoinableForwardingStream::new(ReceiverStream::new(self.rx), self.joinset)
    }
}
