pub(crate) mod stats;
pub(crate) mod task_lifecycle;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{Meter, ops::NodeInfo};
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_local_plan::ExecutionStats;
pub use stats::RuntimeStats;

use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID},
    scheduling::{
        task::{TaskContext, TaskMetadata, TaskName, TaskStatus},
        worker::WorkerId,
    },
    statistics::stats::RuntimeNodeManager,
};

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum TaskEvent {
    Submitted {
        context: TaskContext,
        name: TaskName,
        metadata: TaskMetadata,
    },
    Scheduled {
        context: TaskContext,
        worker_id: WorkerId,
    },
    Completed {
        context: TaskContext,
        stats: ExecutionStats,
        worker_id: WorkerId,
    },
    Failed {
        context: TaskContext,
        reason: String,
        worker_id: Option<WorkerId>,
        retryable: bool,
    },
    Cancelled {
        context: TaskContext,
    },
}

impl TaskEvent {
    pub fn new(context: TaskContext, result: &DaftResult<TaskStatus>, worker_id: WorkerId) -> Self {
        match result {
            Ok(task_status) => match task_status {
                TaskStatus::Success { stats, .. } => Self::Completed {
                    context,
                    stats: stats.clone(),
                    worker_id,
                },
                TaskStatus::Failed { error } => Self::Failed {
                    context,
                    reason: error.to_string(),
                    worker_id: Some(worker_id),
                    retryable: false,
                },
                TaskStatus::Cancelled => Self::Cancelled { context },
                // WorkerDied and WorkerUnavailable are the only
                // task statuses that get retried in the dispatcher
                // src/daft-distributed/src/scheduling/dispatcher.rs
                TaskStatus::WorkerDied => Self::Failed {
                    context,
                    reason: "Worker died".to_string(),
                    worker_id: Some(worker_id),
                    retryable: true,
                },
                TaskStatus::WorkerUnavailable => Self::Failed {
                    context,
                    reason: "Worker unavailable".to_string(),
                    worker_id: Some(worker_id),
                    retryable: true,
                },
            },
            Err(error) => Self::Failed {
                context,
                reason: error.to_string(),
                worker_id: Some(worker_id),
                retryable: false,
            },
        }
    }

    pub fn context(&self) -> &TaskContext {
        match self {
            Self::Submitted { context, .. } => context,
            Self::Scheduled { context, .. } => context,
            Self::Completed { context, .. } => context,
            Self::Failed { context, .. } => context,
            Self::Cancelled { context, .. } => context,
        }
    }
}

pub trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()>;

    /// Called once during [`StatisticsManager`] construction, before any events
    /// are dispatched. Provides shared access to the runtime node managers for
    /// subscribers that need aggregated stats.
    fn set_runtime_node_managers(&mut self, _managers: Arc<HashMap<NodeID, RuntimeNodeManager>>) {}
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    runtime_node_managers: Arc<HashMap<NodeID, RuntimeNodeManager>>,
    subscribers: Mutex<Vec<Box<dyn StatisticsSubscriber>>>,
}

impl StatisticsManager {
    pub fn from_pipeline_node(
        pipeline_node: &DistributedPipelineNode,
        mut subscribers: Vec<Box<dyn StatisticsSubscriber>>,
        meter: &Meter,
    ) -> DaftResult<StatisticsManagerRef> {
        let mut runtime_node_managers = HashMap::new();
        pipeline_node.apply(|node| {
            let node_info = Arc::new(NodeInfo {
                name: node.name(),
                id: node.node_id() as usize,
                node_origin_id: None,
                node_type: node.context().node_type.clone(),
                node_category: node.context().node_category.clone(),
                node_phase: None,
                context: HashMap::new(),
            });
            runtime_node_managers.insert(
                node.node_id(),
                RuntimeNodeManager::new(meter, node.runtime_stats(), node_info),
            );
            Ok(TreeNodeRecursion::Continue)
        })?;

        let runtime_node_managers = Arc::new(runtime_node_managers);

        for subscriber in &mut subscribers {
            subscriber.set_runtime_node_managers(runtime_node_managers.clone());
        }

        Ok(Arc::new(Self {
            runtime_node_managers,
            subscribers: Mutex::new(subscribers),
        }))
    }

    pub fn handle_event(&self, event: TaskEvent) -> DaftResult<()> {
        for node_id in &event.context().node_ids {
            let node_manager = self
                .runtime_node_managers
                .get(node_id)
                .expect("No runtime stats found for node");
            node_manager.handle_task_event(&event);
        }

        let mut subscribers = self.subscribers.lock().unwrap();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            tracing::debug!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }

    /// Collects accumulated stats from each node manager and returns them as an
    /// ExecutionEngineFinalResult for export to the driver (e.g. after the partition stream is done).
    pub fn export_metrics(&self) -> ExecutionStats {
        let nodes: Vec<(Arc<NodeInfo>, _)> = self
            .runtime_node_managers
            .values()
            .map(RuntimeNodeManager::export_snapshot)
            .collect();
        ExecutionStats::new("".into(), nodes)
    }
}
