use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use daft_logical_plan::LogicalPlanRef;

use crate::{
    plan::PlanID,
    scheduling::task::{TaskContext, TaskName, TaskStatus},
};

pub mod http_subscriber;
pub use http_subscriber::HttpSubscriber;

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[cfg(test)]
mod http_subscriber_test;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PlanState {
    pub plan_id: PlanID,
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
    TaskScheduled {
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
        plan_id: PlanID,
        query_id: String,
        logical_plan: LogicalPlanRef,
    },
    PlanStarted {
        plan_id: PlanID,
    },
    PlanFinished {
        plan_id: PlanID,
    },
}

impl StatisticsEvent {
    pub fn plan_id(&self) -> PlanID {
        match self {
            Self::PlanSubmitted { plan_id, .. } => *plan_id,
            Self::PlanStarted { plan_id } => *plan_id,
            Self::PlanFinished { plan_id } => *plan_id,
            Self::TaskSubmitted { context, .. } => context.plan_id,
            Self::TaskScheduled { context } => context.plan_id,
            Self::TaskCompleted { context } => context.plan_id,
            Self::TaskStarted { context } => context.plan_id,
            Self::TaskFailed { context, .. } => context.plan_id,
            Self::TaskCancelled { context } => context.plan_id,
        }
    }
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
    fn handle_event(&mut self, event: &StatisticsEvent) -> DaftResult<()>;
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    subscribers: Mutex<Vec<Box<dyn StatisticsSubscriber>>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self {
            subscribers: Mutex::new(subscribers),
        })
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        let mut subscribers = self.subscribers.lock().unwrap();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }
}
