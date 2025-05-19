use std::{collections::HashMap, future::Future, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::worker::WorkerId;
use crate::utils::channel::OneshotSender;

pub(crate) type TaskId = Arc<str>;
pub(crate) type TaskPriority = u32;
#[allow(dead_code)]
pub(crate) trait Task: Send + Sync + 'static {
    fn priority(&self) -> TaskPriority;
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

    pub fn psets(&self) -> &HashMap<String, Vec<PartitionRef>> {
        &self.psets
    }
}

impl Task for SwordfishTask {
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

pub(crate) trait TaskResultHandle: Send {
    #[allow(dead_code)]
    fn get_result_future(
        &mut self,
    ) -> impl Future<Output = DaftResult<PartitionRef>> + Send + 'static;
}

pub(crate) struct TaskResultHandleAwaiter<H: TaskResultHandle> {
    task_id: TaskId,
    worker_id: WorkerId,
    handle: H,
    result_sender: OneshotSender<DaftResult<PartitionRef>>,
    cancel_token: CancellationToken,
}

impl<H: TaskResultHandle> TaskResultHandleAwaiter<H> {
    pub fn new(
        task_id: TaskId,
        worker_id: WorkerId,
        handle: H,
        result_sender: OneshotSender<DaftResult<PartitionRef>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            task_id,
            worker_id,
            handle,
            result_sender,
            cancel_token,
        }
    }

    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub async fn await_result(mut self) {
        tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {}
            result = self.handle.get_result_future() => {
                if self.result_sender.send(result).is_err() {
                    tracing::debug!("Task result receiver was dropped before task result could be sent");
                }
            }
        }
    }
}
