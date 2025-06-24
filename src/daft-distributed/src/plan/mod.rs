use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_logical_plan::LogicalPlanBuilder;
use serde::{Deserialize, Serialize};

use crate::{
    pipeline_node::MaterializedOutput,
    stage::StagePlan,
    utils::{
        channel::{Receiver, ReceiverStream},
        joinset::JoinSet,
        stream::JoinableForwardingStream,
    },
};

mod runner;
pub(crate) use runner::PlanRunner;

static PLAN_ID_COUNTER: AtomicU16 = AtomicU16::new(0);
pub(crate) type PlanID = u16;

#[derive(Serialize, Deserialize)]
pub(crate) struct DistributedPhysicalPlan {
    id: PlanID,
    stage_plan: StagePlan,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let logical_plan = builder.build();
        let stage_plan = StagePlan::from_logical_plan(logical_plan, config)?;

        Ok(Self {
            id: PLAN_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            stage_plan,
        })
    }

    pub fn id(&self) -> PlanID {
        self.id
    }

    pub fn stage_plan(&self) -> &StagePlan {
        &self.stage_plan
    }
}

pub(crate) type PlanResultStream = JoinableForwardingStream<ReceiverStream<MaterializedOutput>>;

pub(crate) struct PlanResult {
    joinset: JoinSet<DaftResult<()>>,
    rx: Receiver<MaterializedOutput>,
}

impl PlanResult {
    fn new(joinset: JoinSet<DaftResult<()>>, rx: Receiver<MaterializedOutput>) -> Self {
        Self { joinset, rx }
    }

    pub fn into_stream(self) -> PlanResultStream {
        JoinableForwardingStream::new(ReceiverStream::new(self.rx), self.joinset)
    }
}
