use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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

#[derive(Debug)]
#[allow(dead_code)]
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
    TaskStarted {
        context: TaskContext,
    },
    // Additional events for more detailed tracking
    FailedTask {
        context: TaskContext,
        reason: String,
    },
    CancelledTask {
        context: TaskContext,
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
                TaskStatus::Success { .. } => Self::FinishedTask { context },
                TaskStatus::Failed { error } => Self::FailedTask {
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
            Err(error) => Self::FailedTask {
                context,
                reason: error.to_string(),
            },
        }
    }
}

pub trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(
        &self,
        event: &StatisticsEvent,
        plans: &HashMap<u32, PlanState>,
        tasks: &HashMap<TaskContext, TaskState>,
    ) -> DaftResult<()>;

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
    plans: Mutex<HashMap<u32, PlanState>>,
    tasks: Mutex<HashMap<TaskContext, TaskState>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self {
            subscribers,
            plans: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
        })
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

    pub fn register_plan(
        &self,
        plan_id: u32,
        query_id: String,
        logical_plan: LogicalPlanRef,
    ) -> DaftResult<()> {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(
            plan_id,
            PlanState {
                plan_id: plan_id as usize,
                query_id,
                logical_plan,
            },
        );
        Ok(())
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager handling event: {:?}", event);
        // Update internal state based on event
        self.update_state(&event)?;

        // Get current state snapshots
        let plans = self.plans.lock().unwrap().clone();
        let tasks = self.tasks.lock().unwrap().clone();

        tracing::info!(
            target: STATISTICS_LOG_TARGET,
            "StatisticsManager notifying {} subscribers",
            self.subscribers.len()
        );
        // Notify all subscribers
        for (i, subscriber) in self.subscribers.iter().enumerate() {
            tracing::info!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event, &plans, &tasks)?;
        }
        Ok(())
    }

    fn update_state(&self, event: &StatisticsEvent) -> DaftResult<()> {
        let mut tasks = self.tasks.lock().unwrap();

        match event {
            StatisticsEvent::SubmittedTask { context, name } => {
                let task_state = tasks.entry(*context).or_insert_with(|| TaskState {
                    name: name.clone(),
                    status: TaskExecutionStatus::Created,
                    pending: 0,
                    completed: 0,
                    canceled: 0,
                    failed: 0,
                    total: 0,
                });
                task_state.total += 1;
            }
            StatisticsEvent::ScheduledTask { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Running;
                    task_state.pending += 1;
                }
            }
            StatisticsEvent::TaskStarted { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Running;
                }
            }
            StatisticsEvent::FinishedTask { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Completed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.completed += 1;
                }
            }
            StatisticsEvent::FailedTask { context, .. } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Failed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.failed += 1;
                }
            }
            StatisticsEvent::CancelledTask { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Canceled;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.canceled += 1;
                }
            }
            StatisticsEvent::PlanStarted { .. } | StatisticsEvent::PlanFinished { .. } => {
                // Plan-level events don't update task state
            }
        }

        Ok(())
    }
}
