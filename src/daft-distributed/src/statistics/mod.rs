pub(crate) mod stats;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use daft_logical_plan::LogicalPlanRef;

use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::task::{TaskContext, TaskName, TaskStatus},
    statistics::stats::RuntimeStats,
};

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PlanState {
    pub plan_id: PlanID,
    pub query_id: String,
    pub logical_plan: LogicalPlanRef,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum TaskEvent {
    TaskSubmitted {
        context: TaskContext,
        name: TaskName,
    },

    TaskScheduled {
        context: TaskContext,
    },
    TaskCompleted {
        context: TaskContext,
    },
    TaskFailed {
        context: TaskContext,
        reason: String,
    },
    TaskCancelled {
        context: TaskContext,
    },
}

impl TaskEvent {
    pub fn context(&self) -> &TaskContext {
        match self {
            Self::TaskSubmitted { context, .. } => context,
            Self::TaskScheduled { context, .. } => context,
            Self::TaskCompleted { context, .. } => context,
            Self::TaskFailed { context, .. } => context,
            Self::TaskCancelled { context, .. } => context,
        }
    }
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for TaskEvent {
    fn from((context, task_result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match task_result {
            Ok(task_status) => match task_status {
                TaskStatus::Success { .. } => Self::TaskCompleted { context },
                TaskStatus::Failed { error } => Self::TaskFailed {
                    context,
                    reason: error.to_string(),
                },
                TaskStatus::Cancelled => Self::TaskCancelled { context },
                TaskStatus::WorkerDied => Self::TaskFailed {
                    context,
                    reason: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::TaskFailed {
                    context,
                    reason: "Worker unavailable".to_string(),
                },
            },
            Err(error) => Self::TaskFailed {
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
    runtime_stats: HashMap<NodeID, Arc<dyn RuntimeStats>>,
    subscribers: Mutex<Vec<Box<dyn StatisticsSubscriber>>>,
}

impl StatisticsManager {
    pub fn new(
        runtime_stats: HashMap<NodeID, Arc<dyn RuntimeStats>>,
        subscribers: Vec<Box<dyn StatisticsSubscriber>>,
    ) -> StatisticsManagerRef {
        Arc::new(Self {
            runtime_stats,
            subscribers: Mutex::new(subscribers),
        })
    }

    pub fn handle_event(&self, event: TaskEvent) -> DaftResult<()> {
        for node_id in &event.context().node_ids {
            if let Some(runtime_stats) = self.runtime_stats.get(node_id) {
                runtime_stats.handle_task_event(&event)?;
            } else {
                eprintln!("No runtime stats found for node: {:?}", node_id);
            }
        }

        let mut subscribers = self.subscribers.lock().unwrap();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }
}
