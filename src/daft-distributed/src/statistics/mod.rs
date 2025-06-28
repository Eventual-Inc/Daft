use std::sync::Arc;

use common_error::DaftResult;

use crate::scheduling::task::{TaskContext, TaskName, TaskStatus};

#[allow(clippy::enum_variant_names)]
pub(crate) enum StatisticsEvent {
    SubmittedTask {
        context: TaskContext,
        name: TaskName,
    },
    #[allow(dead_code)]
    ScheduledTask {
        context: TaskContext,
    },
    FinishedTask {
        context: TaskContext,
    },
    #[allow(dead_code)]
    FailedTask {
        context: TaskContext,
        reason: String,
    },
    CancelledTask {
        context: TaskContext,
    },
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for StatisticsEvent {
    fn from((context, result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match result {
            Ok(status) => match status {
                TaskStatus::Success { .. } => Self::FinishedTask { context },
                TaskStatus::Failed { error, .. } => Self::FailedTask {
                    context,
                    reason: error.to_string(),
                },
                TaskStatus::Cancelled => Self::CancelledTask { context },
                TaskStatus::WorkerDied => Self::FailedTask {
                    context,
                    reason: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::FailedTask {
                    context,
                    reason: "Worker unavailable".to_string(),
                },
            },
            Err(e) => Self::FailedTask {
                context,
                reason: e.to_string(),
            },
        }
    }
}

pub(crate) trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(&self, event: &StatisticsEvent) -> DaftResult<()>;
}

pub(crate) type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub(crate) struct StatisticsManager {
    subscribers: Vec<Box<dyn StatisticsSubscriber>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self { subscribers })
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        for subscriber in &self.subscribers {
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }
}
