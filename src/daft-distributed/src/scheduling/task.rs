use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;

#[derive(Debug)]
pub struct SwordfishTask {
    plan: LocalPhysicalPlanRef,
    daft_execution_config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
}
impl SwordfishTask {
    #[allow(dead_code)]
    pub fn new(
        plan: LocalPhysicalPlanRef,
        daft_execution_config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
    ) -> Self {
        Self {
            plan,
            daft_execution_config,
            psets,
        }
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn estimated_memory_cost(&self) -> usize {
        self.plan.estimated_memory_cost()
    }

    pub fn execution_config(&self) -> Arc<DaftExecutionConfig> {
        self.daft_execution_config.clone()
    }

    pub fn psets(&self) -> HashMap<String, Vec<PartitionRef>> {
        self.psets.clone()
    }
}

#[async_trait::async_trait]
pub trait SwordfishTaskResultHandle: Send + Sync {
    #[allow(dead_code)]
    async fn get_result(&self) -> DaftResult<PartitionRef>;
}
