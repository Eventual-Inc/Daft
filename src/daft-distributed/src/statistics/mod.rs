pub(crate) mod stats;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{QueryID, ops::NodeInfo};
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_local_plan::ExecutionEngineFinalResult;
use opentelemetry::{InstrumentationScope, KeyValue, global};
pub use stats::RuntimeStats;

use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID},
    scheduling::task::{TaskContext, TaskName, TaskStatus},
    statistics::stats::RuntimeNodeManager,
};

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum TaskEvent {
    Submitted {
        context: TaskContext,
        name: TaskName,
    },

    Scheduled {
        context: TaskContext,
    },
    Completed {
        context: TaskContext,
        stats: ExecutionEngineFinalResult,
    },
    Failed {
        context: TaskContext,
        reason: String,
    },
    Cancelled {
        context: TaskContext,
    },
}

impl TaskEvent {
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

impl From<(TaskContext, &DaftResult<TaskStatus>)> for TaskEvent {
    fn from((context, task_result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match task_result {
            Ok(task_status) => match task_status {
                TaskStatus::Success { stats, .. } => Self::Completed {
                    context,
                    stats: stats.clone(),
                },
                TaskStatus::Failed { error } => Self::Failed {
                    context,
                    reason: error.to_string(),
                },
                TaskStatus::Cancelled => Self::Cancelled { context },
                TaskStatus::WorkerDied => Self::Failed {
                    context,
                    reason: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::Failed {
                    context,
                    reason: "Worker unavailable".to_string(),
                },
            },
            Err(error) => Self::Failed {
                context,
                reason: error.to_string(),
            },
        }
    }
}

pub trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()>;
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    runtime_node_managers: HashMap<NodeID, RuntimeNodeManager>,
    subscribers: Mutex<Vec<Box<dyn StatisticsSubscriber>>>,
}

impl StatisticsManager {
    pub fn from_pipeline_node(
        query_id: QueryID,
        pipeline_node: &DistributedPipelineNode,
        subscribers: Vec<Box<dyn StatisticsSubscriber>>,
    ) -> DaftResult<StatisticsManagerRef> {
        let scope = InstrumentationScope::builder("daft.distributed.node_stats")
            .with_attributes(vec![KeyValue::new("query_id", query_id.to_string())])
            .build();
        let meter = global::meter_with_scope(scope);

        let mut runtime_node_managers = HashMap::new();
        pipeline_node.apply(|node| {
            let node_info = Arc::new(NodeInfo {
                name: node.name().to_string().into(),
                id: node.node_id() as usize,
                node_type: node.context().node_type.clone(),
                node_category: node.context().node_category.clone(),
                context: HashMap::new(),
            });
            runtime_node_managers.insert(
                node.node_id(),
                RuntimeNodeManager::new(&meter, node.runtime_stats(&meter), node_info),
            );
            Ok(TreeNodeRecursion::Continue)
        })?;

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
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }

    /// Collects accumulated stats from each node manager and returns them as an
    /// ExecutionEngineFinalResult for export to the driver (e.g. after the partition stream is done).
    pub fn export_metrics(&self) -> ExecutionEngineFinalResult {
        let nodes: Vec<(Arc<NodeInfo>, _)> = self
            .runtime_node_managers
            .values()
            .map(RuntimeNodeManager::export_snapshot)
            .collect();
        ExecutionEngineFinalResult::new(nodes)
    }
}
