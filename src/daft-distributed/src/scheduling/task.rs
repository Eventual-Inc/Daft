use std::collections::HashMap;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;

#[derive(Debug, Clone)]
pub(crate) enum SchedulingStrategy {
    Spread,
    #[allow(dead_code)]
    NodeAffinity {
        node_id: String,
        soft: bool,
    },
}

#[derive(Debug)]
pub(crate) struct SwordfishTask {
    plan: LocalPhysicalPlanRef,
    psets: HashMap<String, Vec<PartitionRef>>,
    strategy: SchedulingStrategy,
}
impl SwordfishTask {
    #[allow(dead_code)]
    pub fn new(
        plan: LocalPhysicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        strategy: SchedulingStrategy,
    ) -> Self {
        Self {
            plan,
            psets,
            strategy,
        }
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn psets(&self) -> HashMap<String, Vec<PartitionRef>> {
        self.psets.clone()
    }
}

#[async_trait::async_trait]
pub trait SwordfishTaskResultHandle: Send + Sync {
    #[allow(dead_code)]
    async fn get_result(&mut self) -> DaftResult<PartitionRef>;
}
