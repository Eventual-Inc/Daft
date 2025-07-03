use std::sync::Arc;

use common_error::DaftResult;
use daft_logical_plan::LogicalPlanRef;

use crate::scheduling::task::{TaskContext, TaskName, TaskStatus};

pub mod http_subscriber;
pub use http_subscriber::HttpSubscriber;

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[cfg(test)]
mod http_subscriber_test;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PlanState {
    pub plan_id: usize,
    pub query_id: String,
    pub logical_plan: LogicalPlanRef,
}

#[derive(Debug, Clone)]
pub struct TaskState {
    pub name: TaskName,
    pub status: TaskExecutionStatus,
    pub pending: u32,
    pub completed: u32,
    pub canceled: u32,
    pub failed: u32,
    pub total: u32,
}

#[derive(Debug, Clone)]
pub enum TaskExecutionStatus {
    Created,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum StatisticsEvent {
    TaskSubmitted {
        context: TaskContext,
        name: TaskName,
    },
    #[allow(dead_code)]
    ScheduledTask {
        context: TaskContext,
    },
    TaskCompleted {
        context: TaskContext,
    },
    TaskStarted {
        context: TaskContext,
    },
    TaskFailed {
        context: TaskContext,
        reason: String,
    },
    TaskCancelled {
        context: TaskContext,
    },
    PlanSubmitted {
        plan_id: u32,
        query_id: String,
        logical_plan: LogicalPlanRef,
    },
    PlanStarted {
        plan_id: u32,
    },
    PlanFinished {
        plan_id: u32,
    },
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for StatisticsEvent {
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
    fn handle_event(&self, event: &StatisticsEvent) -> DaftResult<()>;

    /// Optional flush method for subscribers that need to clean up pending operations
    fn flush(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = DaftResult<()>> + Send + '_>>>
    {
        None
    }
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    subscribers: Vec<Box<dyn StatisticsSubscriber>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self { subscribers })
    }

    /// Flush all subscribers that support flushing (e.g., HttpSubscriber)
    pub async fn flush_all_subscribers(&self) -> DaftResult<()> {
        tracing::info!(
            target: STATISTICS_LOG_TARGET,
            "Flushing {} subscribers",
            self.subscribers.len()
        );

        for (i, subscriber) in self.subscribers.iter().enumerate() {
            if let Some(flush_future) = subscriber.flush() {
                tracing::info!(
                    target: STATISTICS_LOG_TARGET,
                    "Flushing subscriber {}",
                    i
                );
                flush_future.await?;
            }
        }

        tracing::info!(target: STATISTICS_LOG_TARGET, "All subscribers flushed successfully");
        Ok(())
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        for (i, subscriber) in self.subscribers.iter().enumerate() {
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }
}
