use std::{any::Any, collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use uuid::Uuid;

pub(crate) type TaskId = Arc<str>;
pub(crate) type TaskPriority = u32;
#[allow(dead_code)]
pub(crate) trait Task: Send + Sync + 'static {
    fn priority(&self) -> TaskPriority;
    fn as_any(&self) -> &dyn Any;
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
    id: TaskId,
    plan: LocalPhysicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    strategy: SchedulingStrategy,
}

#[allow(dead_code)]
impl SwordfishTask {
    pub fn new(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        strategy: SchedulingStrategy,
    ) -> Self {
        let task_id = Uuid::new_v4().to_string();
        Self {
            id: Arc::from(task_id),
            plan,
            config,
            psets,
            strategy,
        }
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    pub fn plan(&self) -> LocalPhysicalPlanRef {
        self.plan.clone()
    }

    pub fn estimated_memory_cost(&self) -> usize {
        self.plan.estimated_memory_cost()
    }

    pub fn config(&self) -> &Arc<DaftExecutionConfig> {
        &self.config
    }

    pub fn psets(&self) -> &Arc<HashMap<String, Vec<PartitionRef>>> {
        &self.psets
    }
}

impl Task for SwordfishTask {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn task_id(&self) -> &TaskId {
        &self.id
    }

    fn strategy(&self) -> &SchedulingStrategy {
        &self.strategy
    }

    fn priority(&self) -> TaskPriority {
        // Default priority for now, could be enhanced later
        0
    }
}

pub trait TaskResultHandle: Send + Sync {
    #[allow(dead_code)]
    async fn get_result(&mut self) -> DaftResult<PartitionRef>;
}
