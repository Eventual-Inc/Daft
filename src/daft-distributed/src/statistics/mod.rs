#[cfg(feature = "python")]
pub(crate) mod progress_bar;
pub(crate) mod stats;

use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_metrics::{QueryID, StatSnapshot};
use daft_context::{Subscriber, Subscribers};
use futures::future;

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
    subscribers: Vec<Arc<Subscribers>>,
    #[cfg(feature = "python")]
    progress_bar: Option<progress_bar::FlotillaProgressBar>,
}

impl StatisticsManager {
    pub fn new(
        runtime_stats: HashMap<NodeID, Arc<dyn RuntimeStats>>,
        subscribers: Vec<Arc<Subscribers>>,
    ) -> StatisticsManagerRef {
        #[cfg(feature = "python")]
        {
            use pyo3::Python;

            let progress_bar = Python::attach(|py| {
                Some(
                    progress_bar::FlotillaProgressBar::try_new(py)
                        .expect("Failed to create progress bar"),
                )
            });

            Arc::new(Self {
                runtime_stats,
                subscribers,
                progress_bar,
            })
        }

        #[cfg(not(feature = "python"))]
        {
            Arc::new(Self {
                runtime_stats,
                subscribers,
            })
        }
    }

    pub fn handle_event(&self, event: TaskEvent) -> DaftResult<()> {
        for node_id in &event.context().node_ids {
            let runtime_stats = self
                .runtime_stats
                .get(node_id)
                .expect("No runtime stats found for node");
            runtime_stats.handle_task_event(&event)?;
        }

        #[cfg(feature = "python")]
        {
            if let Some(progress_bar) = &self.progress_bar {
                progress_bar.handle_event(&event)?;
            }
        }

        Ok(())
    }

    pub fn handle_start(&self, query_id: QueryID) -> DaftResult<()> {
        for subscriber in self.subscribers.iter() {
            subscriber.on_exec_start(query_id.clone())?;
        }
        Ok(())
    }

    pub async fn handle_finish(&self, query_id: QueryID) -> DaftResult<()> {
        future::try_join_all(
            self.subscribers
                .iter()
                .map(|subscriber| subscriber.on_exec_end(query_id.clone())),
        )
        .await?;
        Ok(())
    }
}
