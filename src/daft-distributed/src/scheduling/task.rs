use std::{any::Any, collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use uuid::Uuid;

use crate::pipeline_node::MaterializedOutput;
#[derive(Debug, Clone)]
pub(crate) enum SchedulingStrategy {
    Spread,
    #[allow(dead_code)]
    NodeAffinity {
        node_id: String,
        soft: bool,
    },
}

pub(crate) type TaskId = String;

pub(crate) trait Task: Send + Sync + 'static {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn task_id(&self) -> &TaskId;
    fn strategy(&self) -> &SchedulingStrategy;
}

#[derive(Debug)]
pub(crate) struct SwordfishTask {
    id: String,
    plan: LocalPhysicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
    strategy: SchedulingStrategy,
}
impl SwordfishTask {
    #[allow(dead_code)]
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
    async fn get_result(&mut self) -> DaftResult<Vec<MaterializedOutput>>;
    #[allow(dead_code)]
    fn cancel_callback(&mut self) -> DaftResult<()>;
}
