use std::collections::HashMap;

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;

#[derive(Debug)]
pub struct SwordfishTask {
    plan: LocalPhysicalPlanRef,
    psets: HashMap<String, Vec<PartitionRef>>,
}
impl SwordfishTask {
    #[allow(dead_code)]
    pub fn new(plan: LocalPhysicalPlanRef, psets: HashMap<String, Vec<PartitionRef>>) -> Self {
        Self { plan, psets }
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn estimated_memory_cost(&self) -> usize {
        self.plan.estimated_memory_cost()
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
