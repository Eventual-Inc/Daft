pub(crate) mod stats;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::StatSnapshot;

use crate::{
    pipeline_node::NodeID,
    scheduling::task::{TaskContext, TaskName, TaskStatus},
    statistics::stats::RuntimeStats,
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
        stats: Vec<(usize, StatSnapshot)>,
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
            let runtime_stats = self
                .runtime_stats
                .get(node_id)
                .expect("No runtime stats found for node");
            runtime_stats.handle_task_event(&event)?;
        }

        let mut subscribers = self.subscribers.lock().unwrap();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }
}
