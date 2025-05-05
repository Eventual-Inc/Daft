use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, LogicalPlanRef};
use futures::{Stream, StreamExt};

use crate::{
    channel::{create_channel, Receiver, Sender},
    runtime::{get_or_init_runtime, JoinHandle},
    scheduling::worker::WorkerManagerFactory,
    stage::{build_stage_plan, StagePlan, StagePlanBuilder},
};

pub struct DistributedPhysicalPlan {
    stage_plan: StagePlan,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();
        if !can_translate_logical_plan(&plan) {
            return Err(DaftError::InternalError(
                "Cannot run this physical plan on distributed swordfish yet".to_string(),
            ));
        }

        let stage_plan = build_stage_plan(plan, config.clone())?;

        Ok(Self { stage_plan, config })
    }

    pub fn run_plan(
        &self,
        _psets: HashMap<String, Vec<PartitionRef>>,
        _worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> PlanResult {
        let (_result_sender, result_receiver) = create_channel(1);
        let runtime = get_or_init_runtime();
        let handle = runtime.spawn(async move {
            // TODO: FLOTILLA_MS1: Implement plan running loop
            todo!()
        });
        PlanResult::new(handle, result_receiver)
    }

    pub fn execution_config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }
}

fn can_translate_logical_plan(plan: &LogicalPlanRef) -> bool {
    let mut can_translate = true;
    let _ = plan.apply(|node| match node.as_ref() {
        LogicalPlan::Project(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Source(_)
        | LogicalPlan::Sink(_)
        | LogicalPlan::Sample(_)
        | LogicalPlan::Explode(_)
        | LogicalPlan::Unpivot(_) => Ok(TreeNodeRecursion::Continue),
        _ => {
            can_translate = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });
    can_translate
}

// This is the output of a plan, a receiver to receive the results of the plan.
// And the join handle to the task that runs the plan.
pub struct PlanResult {
    _handle: Option<JoinHandle<DaftResult<()>>>,
    _rx: Receiver<PartitionRef>,
}

impl PlanResult {
    fn new(handle: JoinHandle<DaftResult<()>>, rx: Receiver<PartitionRef>) -> Self {
        Self {
            _handle: Some(handle),
            _rx: rx,
        }
    }
}

impl Stream for PlanResult {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("FLOTILLA_MS1: Implement stream for plan result");
    }
}
