use std::{any::Any, collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use uuid::Uuid;

pub(crate) type TaskId = String;

#[allow(dead_code)]
pub(crate) trait Task: Send + Sync + 'static {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn task_id(&self) -> &TaskId;
    fn strategy(&self) -> &SchedulingStrategy;
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum SchedulingStrategy {
    Spread,
    NodeAffinity { node_id: String, soft: bool },
}

#[derive(Debug, Clone)]
pub(crate) struct SwordfishTask {
    id: String,
    plan: LocalPhysicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
    strategy: SchedulingStrategy,
}

#[allow(dead_code)]
impl SwordfishTask {
    pub fn new(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        strategy: SchedulingStrategy,
    ) -> Self {
        let task_id = Uuid::new_v4().to_string();
        Self {
            id: task_id,
            plan,
            config,
            psets,
            strategy,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }

    pub fn psets(&self) -> HashMap<String, Vec<PartitionRef>> {
        self.psets.clone()
    }
}

impl Task for SwordfishTask {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn task_id(&self) -> &TaskId {
        &self.id
    }

    fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }
}

#[async_trait::async_trait]
pub trait SwordfishTaskResultHandle: Send + Sync {
    #[allow(dead_code)]
    async fn get_result(&mut self) -> DaftResult<Vec<PartitionRef>>;
}
